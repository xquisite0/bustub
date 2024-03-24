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
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
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
  timestamp_t txn_id = -1;
  timestamp_t read_ts = -1;
  if (transaction != nullptr) {
    txn_id = transaction->GetTransactionTempTs();
    read_ts = transaction->GetReadTs();
  }

  // initialise the tuple meta
  // TupleMeta old_tuple_meta = {txn_id, true};
  TupleMeta tuple_meta = {txn_id, false};

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
  // std::cout << rid_next << "\n";

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

    // initialise the new tuple
    std::vector<Value> new_values;
    new_values.reserve(target_expressions.size());
    for (AbstractExpressionRef &target_expression : target_expressions) {
      new_values.push_back(target_expression->Evaluate(&tuple_next, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple = Tuple(new_values, &child_executor_->GetOutputSchema());

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
    if (ts == txn_id && !logs_exist) {
      // update tuple
      table_info->table_->UpdateTupleInPlace(tuple_meta, new_tuple, rid_next);
      tuples_updated++;
      transaction->AppendWriteSet(table_oid, rid_next);
      continue;
    }

    // generate schema for the previous log
    // so that we can access the previous tuple state
    std::vector<Column> prev_log_columns;
    int col_id = 0;
    for (bool field : undo_log.modified_fields_) {
      if (field) {
        prev_log_columns.emplace_back(child_executor_->GetOutputSchema().GetColumn(col_id));
      }
      col_id++;
    }
    Schema prev_log_schema(prev_log_columns);

    UndoLog new_undo_log;
    if (ts == txn_id) {
      // tuple was updated by this transaction before
      new_undo_log.is_deleted_ = cur_tuple_meta.is_deleted_;

      int i = 0;
      std::vector<Value> diff_values;
      std::vector<Column> diff_columns;

      int log_tuple_iterator = 0;
      for (bool field : undo_log.modified_fields_) {
        if (!field) {
          Value cur_val = tuple_next.GetValue(&child_executor_->GetOutputSchema(), i);
          Value next_val = new_tuple.GetValue(&child_executor_->GetOutputSchema(), i);
          if (!cur_val.CompareExactlyEquals(next_val)) {
            diff_values.emplace_back(cur_val);
            diff_columns.emplace_back(child_executor_->GetOutputSchema().GetColumn(i));
          }
          new_undo_log.modified_fields_.emplace_back(!cur_val.CompareExactlyEquals(next_val));
        } else {
          diff_values.emplace_back(undo_log.tuple_.GetValue(&prev_log_schema, log_tuple_iterator));
          diff_columns.emplace_back(child_executor_->GetOutputSchema().GetColumn(i));
          new_undo_log.modified_fields_.emplace_back(true);
          log_tuple_iterator++;
        }
        i++;
      }
      Schema diff_schema = Schema(diff_columns);
      new_undo_log.tuple_ = Tuple(diff_values, &diff_schema);
      new_undo_log.ts_ = undo_log.ts_;
      new_undo_log.prev_version_ = undo_log.prev_version_;

    } else {
      std::vector<Value> diff_values;
      std::vector<Column> diff_columns;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        Value cur_val = tuple_next.GetValue(&child_executor_->GetOutputSchema(), i);
        Value next_val = new_tuple.GetValue(&child_executor_->GetOutputSchema(), i);
        if (!cur_val.CompareExactlyEquals(next_val)) {
          diff_values.emplace_back(cur_val);
          diff_columns.emplace_back(child_executor_->GetOutputSchema().GetColumn(i));
        }
        new_undo_log.modified_fields_.emplace_back(!cur_val.CompareExactlyEquals(next_val));
      }
      Schema diff_schema = Schema(diff_columns);
      new_undo_log.tuple_ = Tuple(diff_values, &diff_schema);
      new_undo_log.ts_ = ts;
      new_undo_log.prev_version_ = undo_link;
    }

    // just modify the existing undolog
    if (ts == txn_id) {
      // std::cout << "Updated transaction " << txn_id - TXN_START_ID << " at Log ID: " << undo_link.prev_log_idx_ <<
      // "\n"; std::cout << "The undo log's modified fields size is: " << new_undo_log.modified_fields_.size() << "\n";
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
      Tuple old_key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      Tuple new_key =
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, rid_next, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key, rid_next, exec_ctx_->GetTransaction());
    }
    transaction->AppendWriteSet(table_oid, rid_next);

    // ENDCOPY -----------
    // delete old tuple
    // table_info->table_->UpdateTupleMeta(old_tuple_meta, rid_next);

    // update tuple columns
    // std::vector<Value> new_values;
    // new_values.reserve(target_expressions.size());
    // for (AbstractExpressionRef &target_expression : target_expressions) {
    //   // std::cout << &GetOutputSchema() << "\n";
    //   new_values.push_back(target_expression->Evaluate(&tuple_next, child_executor_->GetOutputSchema()));
    // }
    // std::cout << tuple_next.ToString(&child_executor_->GetOutputSchema()) << "\n";

    // insert tuple into table
    // Tuple new_tuple = Tuple(new_values, &child_executor_->GetOutputSchema());

    // std::optional<RID> rid_of_inserted = table_info->table_->InsertTuple(
    // new_tuple_meta, new_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_oid);
  }

  std::vector<Value> values = {Value(TypeId::INTEGER, tuples_updated)};

  *tuple = Tuple(values, &GetOutputSchema());
  updated_ = true;
  // std::cout << "We added " << tuples_added << " tuples\n";
  return true;
}

}  // namespace bustub
