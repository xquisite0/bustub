//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (exec_ctx != nullptr && exec_ctx->GetTransaction() != nullptr) {
    read_ts_ = exec_ctx->GetTransaction()->GetReadTs();
    temp_ts_ = exec_ctx->GetTransaction()->GetTransactionTempTs();
  }
}

void SeqScanExecutor::Init() {
  // obtain table info
  table_oid_t table_oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);

  // obtain table iterator and assign as an attribute
  table_iterator_ = std::make_shared<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // reference table iterator to provide next tuple
  // repeats until we reach the end of table or we find a non-deleted tuple

  if (table_iterator_ == nullptr) {
    Init();
  }

  // obtain the predicates if any
  AbstractExpressionRef filter_predicate = plan_->filter_predicate_;

  while (true) {
    // if iterator has reach its end, there are no more tuples in the table, return false
    // std::cout << table_iterator_ << "\n";
    if (table_iterator_->IsEnd()) {
      return false;
    }

    std::pair<TupleMeta, Tuple> tuple_meta_and_tuple = table_iterator_->GetTuple();
    RID cur_rid = table_iterator_->GetRID();
    TupleMeta tuple_meta = tuple_meta_and_tuple.first;
    Tuple cur_tuple = tuple_meta_and_tuple.second;
    ++(*table_iterator_);
    timestamp_t ts = tuple_meta.ts_;

    // Case 1: tuple in heap is the most recent data, and can be accessed by current txn.
    if (ts < TXN_START_ID && read_ts_ >= ts) {
      // carry on as per usual
      // std::cout << "Case 1 ";
    }

    // Case 2: tuple was modified earlier in our same transaction. So it is uncommitted, but we can access it.
    if (ts == temp_ts_) {
      // carry on as per usual
      // std::cout << "Case 2 ";
    }

    // Case 3: the tricky case. The tuple is either 1) uncommitted from another transaction or 2) is beyond our current
    // read_ts (this tuple is a value from the future!)
    if ((ts >= TXN_START_ID && temp_ts_ != ts) || (ts < TXN_START_ID && read_ts_ < ts)) {
      // std::cout << "Case 3 ";
      // iterate the version chain to obtain the undo logs
      TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
      if (txn_mgr == nullptr) {
        // handle
      }
      std::optional<UndoLink> undo_link_opt = txn_mgr->GetUndoLink(cur_rid);
      UndoLink undo_link;
      if (!undo_link_opt.has_value() || !undo_link_opt->IsValid()) {
        // handle
      } else {
        undo_link = *undo_link_opt;
      }
      bool found_readable = false;
      std::vector<UndoLog> undo_logs;
      while (true) {
        if (!undo_link.IsValid()) {
          break;
        }
        UndoLog cur_undo_log = txn_mgr->GetUndoLog(undo_link);
        undo_logs.emplace_back(cur_undo_log);
        if (read_ts_ >= cur_undo_log.ts_) {
          found_readable = true;
          break;
        }
        undo_link = cur_undo_log.prev_version_;
      }

      if (!found_readable) {
        tuple_meta.is_deleted_ = true;
        continue;
      }
      // reconstruct the tuple
      std::optional<Tuple> cur_tuple_opt = ReconstructTuple(&GetOutputSchema(), cur_tuple, tuple_meta, undo_logs);

      if (cur_tuple_opt.has_value()) {
        cur_tuple = *cur_tuple_opt;
        tuple_meta.is_deleted_ = false;
      } else {
        tuple_meta.is_deleted_ = true;
      }

      // qn: how do we determine if the tuple is a deleted one... let's worry about that later, seems like we handled
      // that case in our tuple reconstruction already
    }

    if (tuple_meta.is_deleted_) {
      continue;
    }

    if (filter_predicate != nullptr) {
      // if the row fails the predicate, move on to the next tuple
      Value fulfills_predicate = filter_predicate->Evaluate(&cur_tuple, GetOutputSchema());
      if (fulfills_predicate.CompareEquals(Value(TypeId::BOOLEAN, 1)) == CmpBool::CmpFalse) {
        continue;
      }
    }

    // tuple is valid
    *tuple = cur_tuple;
    *rid = cur_rid;
    break;
  }
  return true;
}

}  // namespace bustub
