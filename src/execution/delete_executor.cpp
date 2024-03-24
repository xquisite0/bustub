//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // ensures that Next() only returns true with number of deleted rows ONCE
  if (deleted_) {
    return false;
  }
  // obtain table info
  table_oid_t table_oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);
  Transaction *transaction = exec_ctx_->GetTransaction();
  timestamp_t txn_id = -1;
  timestamp_t read_ts = -1;
  if (transaction != nullptr) {
    txn_id = transaction->GetTransactionTempTs();
    read_ts = transaction->GetReadTs();
  }
  // initialise the tuple meta
  TupleMeta tuple_meta = {txn_id, true};

  // obtain the list of table_indexes
  std::vector<IndexInfo *> table_indexes = catalog->GetTableIndexes(table_info->name_);

  // deletes tuples from the table + updates indexes

  // grab the tuples from child executor and store in local buffer
  int tuples_deleted = 0;
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};

  while (child_executor_->Next(&tuple_next, &rid_next)) {
    std::pair<Tuple, RID> p = {tuple_next, rid_next};
    child_tuples_.emplace_back(p);
  }

  // process each tuple
  for (auto &p : child_tuples_) {
    auto [tuple_next, rid_next] = p;

    TupleMeta cur_tuple_meta = table_info->table_->GetTupleMeta(rid_next);
    timestamp_t ts = cur_tuple_meta.ts_;

    // process w-w conflicts!
    // Case 1 of w-w conflict: tuple has been modified by another uncommitted transaction
    if (ts >= TXN_START_ID && ts != txn_id) {
      std::cout << "Exception thrown.\n";
      transaction->SetTainted();
      throw ExecutionException("w-w conflict: tuple has been modified by another uncommitted transaction");
    }

    // Case 2 of w-w conflict: tuple has been modified by a committed transaction in the "future" (timestamp > our
    // current readable timestamp)
    if (ts < TXN_START_ID && ts > read_ts) {
      transaction->SetTainted();
      throw ExecutionException(
          "w-w conflict: tuple has been modified by a committed transaction in the 'future' "
          "(timestamp > our current readable timestamp)");
    }
    if (ts == txn_id) {
      table_info->table_->UpdateTupleMeta(tuple_meta, rid_next);

      tuples_deleted++;
      transaction->AppendWriteSet(table_oid, rid_next);
      continue;
    }

    // grab the most recent undo log for the current tuple
    bool logs_exist = true;
    UndoLink undo_link;
    TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
    if (txn_mgr != nullptr) {
      auto undo_link_opt = txn_mgr->GetUndoLink(rid_next);
      if (undo_link_opt.has_value()) {
        undo_link = undo_link_opt.value();
      } else {
        logs_exist = false;
      }
    }
    UndoLog undo_log;
    if (logs_exist && txn_mgr != nullptr) {
      undo_log = txn_mgr->GetUndoLog(undo_link);
    }

    // create the new undolog
    UndoLog new_undo_log;
    new_undo_log.is_deleted_ = cur_tuple_meta.is_deleted_;
    for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
      // this is supposed to append all trues.
      new_undo_log.modified_fields_.emplace_back(true);
    }
    new_undo_log.tuple_ = tuple_next;
    new_undo_log.ts_ = ts;
    new_undo_log.prev_version_ = undo_link;

    // just modify the existing undolog
    if (undo_log.ts_ == txn_id) {
      transaction->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
    } else {
      UndoLink new_undo_link = transaction->AppendUndoLog(new_undo_log);
      txn_mgr->UpdateUndoLink(rid_next, new_undo_link);

      // delete tuple
      table_info->table_->UpdateTupleMeta(tuple_meta, rid_next);

      tuples_deleted++;
    }

    // update indices
    for (IndexInfo *index_info : table_indexes) {
      Tuple old_key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, rid_next, exec_ctx_->GetTransaction());
    }
    transaction->AppendWriteSet(table_oid, rid_next);
  }

  std::vector<Value> values = {Value(TypeId::INTEGER, tuples_deleted)};

  *tuple = Tuple(values, &GetOutputSchema());
  deleted_ = true;
  // std::cout << "We added " << tuples_added << " tuples\n";
  return true;
}

}  // namespace bustub
