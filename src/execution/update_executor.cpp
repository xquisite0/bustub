//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "binder/keyword_helper.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  if (exec_ctx != nullptr && exec_ctx->GetTransaction() != nullptr) {
    read_ts_ = exec_ctx->GetTransaction()->GetReadTs();
    txn_id_ = exec_ctx->GetTransaction()->GetTransactionTempTs();
  }
}

void UpdateExecutor::Init() {}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // ensures that Next() only returns true with number of updated rows ONCE
  if (updated_) {
    return false;
  }
  // obtain table info
  table_oid_t table_oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);

  // get the transaction id & read timestamp of cur txn
  Transaction *transaction = exec_ctx_->GetTransaction();
  if (transaction == nullptr) {
    return false;
  }

  std::vector<Tuple> to_insert;

  // initialise the tuple meta
  TupleMeta tuple_meta = {txn_id_, false};

  // obtain the list of table_indexes
  std::vector<IndexInfo *> table_indexes = catalog->GetTableIndexes(table_info->name_);

  // obtain the update expressions
  std::vector<AbstractExpressionRef> target_expressions = plan_->target_expressions_;

  // update tuples into the table + update indexes
  int tuples_updated = 0;
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};

  while (child_executor_->Next(&tuple_next, &rid_next)) {
    std::pair<Tuple, RID> p = {tuple_next, rid_next};
    child_tuples_.emplace_back(p);
  }
  std::cout << "SIZE of tuples to process : " << child_tuples_.size() << "\n";
  // std::cout << rid_next << "\n";

  for (auto &p : child_tuples_) {
    auto [tuple_next, rid_next] = p;

    TupleMeta cur_tuple_meta = table_info->table_->GetTupleMeta(rid_next);
    timestamp_t ts = cur_tuple_meta.ts_;

    ProcessWriteWriteConflict(ts, read_ts_, txn_id_, transaction);

    // initialise the new tuple
    std::vector<Value> new_values;
    new_values.reserve(target_expressions.size());

    for (AbstractExpressionRef &target_expression : target_expressions) {
      new_values.push_back(target_expression->Evaluate(&tuple_next, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple = Tuple(new_values, &child_executor_->GetOutputSchema());

    // check whether this tuple has its primary key changed.
    bool to_skip = false;
    for (IndexInfo *index_info : table_indexes) {
      Tuple old_key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      Tuple new_key =
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      // primary key has changed, proceed to delete the element
      if (!IsTupleContentEqual(old_key, new_key)) {
        std::cout << "Processing tuple : " << tuple_next.ToString(&child_executor_->GetOutputSchema()) << "\n";
        TupleMeta new_tuple_meta = {txn_id_, true};
        TupleMeta cur_tuple_meta = table_info->table_->GetTupleMeta(rid_next);
        timestamp_t ts = cur_tuple_meta.ts_;

        ProcessWriteWriteConflict(ts, read_ts_, txn_id_, transaction);

        // at this point we are cleared of w-w conflicts!

        // let's check for self-modification now.
        // for now we just update tuplemeta to show that it's deleted
        // no need to update the most recent undolog, because the original tuple is still stored in the tableheap,
        // making tuple reconstruction still possible.
        if (ts == txn_id_) {
          table_info->table_->UpdateTupleMeta(tuple_meta, rid_next);
          tuples_updated++;
          transaction->AppendWriteSet(table_oid, rid_next);
          to_skip = true;
          continue;
        }

        // at this point we are deleting a tuple that requires us to create a new undolog, and add it to the version
        // chain

        // insert new undo log into version chain. typical linkedlist insert algorithm.
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
        // make the new undo log point towards the current most recent undo log.
        new_undo_log.prev_version_ = undo_link;

        // just modify the existing undolog
        // is it even possible for an undo log to have an uncommitted timestamp?
        // if (undo_log.ts_ == txn_id) {
        //   transaction->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
        // } else {

        UndoLink new_undo_link = transaction->AppendUndoLog(new_undo_log);
        txn_mgr->UpdateUndoLink(rid_next, new_undo_link);

        // delete tuple
        table_info->table_->UpdateTupleMeta(new_tuple_meta, rid_next);

        // tuples_updated++;
        transaction->AppendWriteSet(table_oid, rid_next);

        // ADD RELEVANT INFO TO A QUEUE

        to_insert.emplace_back(new_tuple);

        to_skip = true;
      }
    }
    if (to_skip) {
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

    // newly inserted by this transaction
    if (ts == txn_id_ && !logs_exist) {
      // update tuple
      table_info->table_->UpdateTupleInPlace(tuple_meta, new_tuple, rid_next);
      tuples_updated++;
      transaction->AppendWriteSet(table_oid, rid_next);
      continue;
    }

    Schema prev_log_schema = GetUndoLogSchema(undo_log, child_executor_->GetOutputSchema());

    UndoLog new_undo_log = GenerateDiffLog(ts, txn_id_, cur_tuple_meta, undo_log, tuple_next, new_tuple,
                                           child_executor_->GetOutputSchema(), prev_log_schema, undo_link);

    // just modify the existing undolog
    if (ts == txn_id_) {
      transaction->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
      table_info->table_->UpdateTupleInPlace(tuple_meta, new_tuple, rid_next);
    } else {
      UndoLink new_undo_link = transaction->AppendUndoLog(new_undo_log);
      txn_mgr->UpdateUndoLink(rid_next, new_undo_link);

      // update tuple & meta
      table_info->table_->UpdateTupleInPlace(tuple_meta, new_tuple, rid_next);
    }
    tuples_updated++;
    // update indices
    for (IndexInfo *index_info : table_indexes) {
      // std::cout << "RAN\n\n";
      Tuple old_key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      Tuple new_key =
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, rid_next, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key, rid_next, exec_ctx_->GetTransaction());
    }
    transaction->AppendWriteSet(table_oid, rid_next);
  }

  for (Tuple &tuple_next : to_insert) {
    std::cout << "Inserting tuple: " << tuple_next.ToString(&child_executor_->GetOutputSchema()) << "\n";
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
          std::cout << tuple_opt.value().ToString(&child_executor_->GetOutputSchema()) << "\n";
          throw ExecutionException("write-write conflict in primary key");
          continue;
        }
      }
    }

    // if this tuple points to an index that is already deleted, we effectively perform an update
    if (deleted_index) {
      TupleMeta cur_tuple_meta = table_info->table_->GetTupleMeta(rid_of_deleted);
      timestamp_t ts = cur_tuple_meta.ts_;

      ProcessWriteWriteConflict(ts, read_ts_, txn_id_, transaction);

      bool logs_exist = true;
      UndoLink undo_link;
      TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
      if (txn_mgr != nullptr) {
        auto undo_link_opt = txn_mgr->GetUndoLink(rid_of_deleted);
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

      if (ts == txn_id_ && !logs_exist) {
        // update tuple
        table_info->table_->UpdateTupleInPlace(tuple_meta, tuple_next, rid_of_deleted);
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
        txn_mgr->UpdateUndoLink(rid_of_deleted, new_undo_link);

        // update tuple & meta
        table_info->table_->UpdateTupleInPlace(tuple_meta, tuple_next, rid_of_deleted);
      }
      transaction->AppendWriteSet(table_oid, rid_of_deleted);
      tuples_updated++;
      continue;
    }
    // std::cout << "INSERTED\n";
    std::optional<RID> rid_of_inserted =
        table_info->table_->InsertTuple(tuple_meta, tuple_next, exec_ctx_->GetLockManager(), transaction, table_oid);

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

  std::vector<Value> values = {Value(TypeId::INTEGER, tuples_updated)};

  *tuple = Tuple(values, &GetOutputSchema());
  updated_ = true;
  // std::cout << "We added " << tuples_added << " tuples\n";
  return true;
}

}  // namespace bustub
