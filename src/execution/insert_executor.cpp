//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/column.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (exec_ctx != nullptr && exec_ctx->GetTransaction() != nullptr) {
    read_ts_ = exec_ctx->GetTransaction()->GetReadTs();
    txn_id_ = exec_ctx->GetTransaction()->GetTransactionTempTs();
  }
}

void InsertExecutor::Init() {
  // std::cout << child_executor_->GetOutputSchema();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // ensures that Next() only returns true with number of inserted rows ONCE
  if (inserted_) {
    return false;
  }
  Transaction *transaction = exec_ctx_->GetTransaction();
  if (transaction == nullptr) {
    return false;
  }
  // obtain table info
  table_oid_t table_oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);

  // initialise the tuple meta
  TupleMeta tuple_meta = {exec_ctx_->GetTransaction()->GetTransactionTempTs(), false};

  // obtain the list of table_indexes
  std::vector<IndexInfo *> table_indexes = catalog->GetTableIndexes(table_info->name_);

  // add tuples into the table + update indexes

  int tuples_added = 0;
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};
  // std::cout << rid_next << "\n";

  while (child_executor_->Next(&tuple_next, &rid_next)) {
    bool deleted_index = false;
    RID rid_of_deleted;
    for (IndexInfo *index_info : table_indexes) {
      // construct key
      Tuple key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      // check if key exists in index already
      std::vector<RID> result;
      index_info->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());

      if (!result.empty()) {
        // check whether the current tuple is deleted or not
        RID cur_rid = result[0];
        std::optional<Tuple> tuple_opt = GetTuple(table_info->table_->GetTuple(cur_rid), cur_rid, read_ts_, txn_id_,
                                                  exec_ctx_, child_executor_->GetOutputSchema());

        if (!tuple_opt.has_value()) {
          deleted_index = true;
          rid_of_deleted = cur_rid;
        } else {
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("write-write conflict in primary key");
          continue;
        }
      }
    }

    // if this tuple points to an index that is already deleted, we effectively perform an update
    if (deleted_index) {
      VersionUndoLink version_undo_link;
      TupleMeta cur_tuple_meta = table_info->table_->GetTupleMeta(rid_of_deleted);
      timestamp_t ts = cur_tuple_meta.ts_;

      ProcessWriteWriteConflict(ts, read_ts_, txn_id_, transaction);

      bool logs_exist = true;
      UndoLink undo_link;
      TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
      if (txn_mgr != nullptr) {
        auto version_undo_link_opt = txn_mgr->GetVersionLink(rid_of_deleted);
        if (version_undo_link_opt.has_value()) {
          version_undo_link = version_undo_link_opt.value();
          if (version_undo_link.in_progress_) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("write-write conflict in primary key");
          }
          version_undo_link.in_progress_ = true;
          txn_mgr->UpdateVersionLink(rid_of_deleted, version_undo_link);
          undo_link = version_undo_link.prev_;
        } else {
          logs_exist = false;
        }
      }
      UndoLog undo_log;
      if (logs_exist && txn_mgr != nullptr) {
        undo_log = txn_mgr->GetUndoLog(undo_link);
      }

      if (ts == txn_id_ && !logs_exist) {
        // update tuple
        table_info->table_->UpdateTupleInPlace(tuple_meta, tuple_next, rid_of_deleted);
        tuples_added++;
        transaction->AppendWriteSet(table_oid, rid_of_deleted);
        // version_undo_link.in_progress_ = false;
        // txn_mgr->UpdateVersionLink(rid_of_deleted, version_undo_link);
        continue;
      }

      Schema prev_log_schema = GetUndoLogSchema(undo_log, child_executor_->GetOutputSchema());

      UndoLog new_undo_log =
          GenerateDiffLog(ts, txn_id_, cur_tuple_meta, undo_log, table_info->table_->GetTuple(rid_of_deleted).second,
                          tuple_next, child_executor_->GetOutputSchema(), prev_log_schema, undo_link);

      // just modify the existing undolog
      if (ts == txn_id_) {
        transaction->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
        table_info->table_->UpdateTupleInPlace(tuple_meta, tuple_next, rid_of_deleted);
      } else {
        UndoLink new_undo_link = transaction->AppendUndoLog(new_undo_log);
        VersionUndoLink new_version_undo_link;
        new_version_undo_link.in_progress_ = true;
        new_version_undo_link.prev_ = new_undo_link;
        version_undo_link = new_version_undo_link;
        txn_mgr->UpdateVersionLink(rid_of_deleted, version_undo_link);

        // update tuple & meta
        table_info->table_->UpdateTupleInPlace(tuple_meta, tuple_next, rid_of_deleted);
      }
      transaction->AppendWriteSet(table_oid, rid_of_deleted);
      tuples_added++;

      version_undo_link.in_progress_ = false;
      txn_mgr->UpdateVersionLink(rid_of_deleted, version_undo_link);

      // test
      // auto sample_opt = txn_mgr->GetVersionLink(rid_of_deleted);
      // if (sample_opt.has_value()) {
      //   VersionUndoLink sample = sample_opt.value();
      //   std::cout << "The below has in progress value of " << sample.in_progress_ << "\n";
      // }
      // std::cout << "We are done inserting rid with deleted index : " << rid_of_deleted << "\n";
      // endtest

      continue;
    }
    std::optional<RID> rid_of_inserted =
        table_info->table_->InsertTuple(tuple_meta, tuple_next, exec_ctx_->GetLockManager(), transaction, table_oid);
    tuples_added++;
    ;
    // update indices
    for (IndexInfo *index_info : table_indexes) {
      // construct key
      Tuple key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      if (rid_of_inserted.has_value() && !index_info->index_->InsertEntry(key, rid_of_inserted.value(), transaction)) {
        throw ExecutionException("write-write conflict in primary key");
        exec_ctx_->GetTransaction()->SetTainted();
      }
    }

    // update write set of transaction
    if (rid_of_inserted.has_value()) {
      transaction->AppendWriteSet(table_oid, rid_of_inserted.value());
    }
  }

  std::vector<Value> values = {Value(TypeId::INTEGER, tuples_added)};

  *tuple = Tuple(values, &GetOutputSchema());
  inserted_ = true;
  // std::cout << "We added " << tuples_added << " tuples\n";
  return true;
}

}  // namespace bustub
