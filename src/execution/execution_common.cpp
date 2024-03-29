#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto GetTuple(const std::pair<TupleMeta, Tuple> &tuple_meta_and_tuple, RID cur_rid, timestamp_t read_ts,
              timestamp_t temp_ts, ExecutorContext *exec_ctx, const Schema &schema,
              const AbstractExpressionRef &filter_predicate) -> std::optional<Tuple> {
  TupleMeta tuple_meta = tuple_meta_and_tuple.first;
  Tuple cur_tuple = tuple_meta_and_tuple.second;
  timestamp_t ts = tuple_meta.ts_;

  // Case 1: tuple in heap is the most recent data, and can be accessed by current txn.
  if (ts < TXN_START_ID && read_ts >= ts) {
    // carry on as per usual
    // std::cout << "Case 1 ";
  }

  // Case 2: tuple was modified earlier in our same transaction. So it is uncommitted, but we can access it.
  if (ts == temp_ts) {
    // carry on as per usual
    // std::cout << "Case 2 ";
  }

  // Case 3: the tricky case. The tuple is either 1) uncommitted from another transaction or 2) is beyond our current
  // read_ts (this tuple is a value from the future!)
  if ((ts >= TXN_START_ID && temp_ts != ts) || (ts < TXN_START_ID && read_ts < ts)) {
    // std::cout << "Case 3 ";
    // iterate the version chain to obtain the undo logs
    TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
    if (txn_mgr == nullptr) {
      // handle
      return std::nullopt;
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
      if (read_ts >= cur_undo_log.ts_) {
        found_readable = true;
        break;
      }
      undo_link = cur_undo_log.prev_version_;
    }

    if (!found_readable) {
      tuple_meta.is_deleted_ = true;
      return std::nullopt;
    }
    // reconstruct the tuple
    std::optional<Tuple> cur_tuple_opt = ReconstructTuple(&schema, cur_tuple, tuple_meta, undo_logs);

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
    return std::nullopt;
  }

  if (filter_predicate != nullptr) {
    // if the row fails the predicate, move on to the next tuple
    Value fulfills_predicate = filter_predicate->Evaluate(&cur_tuple, schema);
    if (fulfills_predicate.CompareEquals(Value(TypeId::BOOLEAN, 1)) == CmpBool::CmpFalse) {
      return std::nullopt;
    }
  }
  return cur_tuple;
}
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  bool is_deleted = base_meta.is_deleted_;

  int column_count = schema->GetColumnCount();
  Tuple output_tuple = base_tuple;

  for (const UndoLog &undo_log : undo_logs) {
    is_deleted = undo_log.is_deleted_;

    std::vector<bool> modified_fields = undo_log.modified_fields_;
    Tuple log_tuple = undo_log.tuple_;

    // Generate the partial schema that allows us to extract the log's tuple values
    std::vector<Column> partial_columns;
    for (int col_id = 0; col_id < column_count; col_id++) {
      if (!modified_fields[col_id]) {
        continue;
      }
      Column column = schema->GetColumn(col_id);
      partial_columns.emplace_back(column);
    }
    Schema partial_schema{partial_columns};

    // Extract the tuple values and rewrite our base tuple
    std::vector<Value> new_values;
    // Iterator that allows us to extract the value from the undo log tuple
    int partial_tuple_iterator = 0;
    for (int col_id = 0; col_id < column_count; col_id++) {
      if (!modified_fields[col_id]) {
        Value value = output_tuple.GetValue(schema, col_id);
        new_values.emplace_back(value);
      } else {
        Value value = log_tuple.GetValue(&partial_schema, partial_tuple_iterator);
        new_values.emplace_back(value);
        partial_tuple_iterator++;
      }
    }
    Tuple new_tuple(new_values, schema);
    output_tuple = std::move(new_tuple);
  }
  if (is_deleted) {
    return std::nullopt;
  }
  return output_tuple;
}

void Helper(TransactionManager *txn_mgr, RID cur_rid, const TableInfo *table_info, Tuple *base_tuple) {
  // UndoLink undo_link;
  std::optional<UndoLink> undo_link_opt = txn_mgr->GetUndoLink(cur_rid);

  if (!undo_link_opt.has_value() || !undo_link_opt->IsValid()) {
    return;
  }
  Schema schema = table_info->schema_;

  int column_count = schema.GetColumnCount();
  Tuple output_tuple = *base_tuple;
  UndoLink undo_link = *undo_link_opt;

  timestamp_t ts = table_info->table_->GetTupleMeta(cur_rid).ts_;
  if (ts < TXN_START_ID && txn_mgr->GetWatermark() >= ts) {
    return;
  }
  while (undo_link.IsValid()) {
    UndoLog undo_log = txn_mgr->GetUndoLog(undo_link);
    // is_deleted = undo_log.is_deleted_;
    std::cout << "\t";
    std::cout << "txn" << undo_link.prev_txn_ - TXN_START_ID << "@" << undo_link.prev_log_idx_ << " ";
    // std::cout << "prev_txn_: " << undo_link.prev_txn_ - TXN_START_ID << " "
    // << "prev_log_idx_: " << undo_link.prev_log_idx_ << "\n";

    // std::cout << "ts_: " << undo_log.ts_ << "\n";
    // std::cout << "Size of modified_fields_: " << undo_log.modified_fields_.size() << "\n";

    // Generate the cur tuple
    // Generate the partial schema that allows us to extract the log's tuple values
    std::vector<Column> partial_columns;
    for (int col_id = 0; col_id < column_count; col_id++) {
      if (!undo_log.modified_fields_[col_id]) {
        continue;
      }
      Column column = schema.GetColumn(col_id);
      partial_columns.emplace_back(column);
    }
    Schema partial_schema{partial_columns};
    // Extract the tuple values and rewrite our base tuple
    std::vector<Value> new_values;
    Tuple log_tuple = undo_log.tuple_;
    // Iterator that allows us to extract the value from the undo log tuple
    int partial_tuple_iterator = 0;
    for (int col_id = 0; col_id < column_count; col_id++) {
      if (!undo_log.modified_fields_[col_id]) {
        Value value = output_tuple.GetValue(&schema, col_id);
        new_values.emplace_back(value);
      } else {
        Value value = log_tuple.GetValue(&partial_schema, partial_tuple_iterator);
        new_values.emplace_back(value);
        partial_tuple_iterator++;
      }
    }
    Tuple new_tuple(new_values, &schema);
    output_tuple = std::move(new_tuple);

    if (undo_log.is_deleted_) {
      std::cout << "<del> ";
    } else {
      std::cout << output_tuple.ToString(&table_info->schema_) << " ";
    }
    std::cout << "ts=" << undo_log.ts_ << "\n";
    if (txn_mgr->GetWatermark() >= undo_log.ts_) {
      break;
    }
    undo_link = undo_log.prev_version_;
  }
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  // fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1

  TableIterator iterator = table_heap->MakeIterator();
  while (!iterator.IsEnd()) {
    RID cur_rid = iterator.GetRID();
    auto [tuple_meta, tuple] = table_heap->GetTuple(cur_rid);

    std::cout << "RID=" << cur_rid.GetPageId() << "/" << cur_rid.GetSlotNum() << " ";
    std::cout << "ts=";
    if (tuple_meta.ts_ >= TXN_START_ID) {
      std::cout << "txn" << tuple_meta.ts_ - TXN_START_ID << " ";
    } else {
      std::cout << tuple_meta.ts_ << " ";
    }
    if (tuple_meta.is_deleted_) {
      std::cout << "deleted ";
    }
    std::cout << "tuple=" << tuple.ToString(&table_info->schema_) << "\n";

    Helper(txn_mgr, cur_rid, table_info, &tuple);
    ++iterator;
  }
}

auto ProcessWriteWriteConflict(timestamp_t ts, timestamp_t read_ts, timestamp_t txn_id, Transaction *transaction)
    -> void {
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
}

auto GenerateDiffLog(timestamp_t ts, timestamp_t txn_id, TupleMeta cur_tuple_meta, const UndoLog &undo_log,
                     const Tuple &tuple_next, const Tuple &new_tuple, const Schema &schema,
                     const Schema &prev_log_schema, UndoLink undo_link) -> UndoLog {
  UndoLog new_undo_log;
  if (ts == txn_id) {
    // tuple was updated by this transaction before
    new_undo_log.is_deleted_ = undo_log.is_deleted_;
    int i = 0;
    std::vector<Value> diff_values;
    std::vector<Column> diff_columns;

    int log_tuple_iterator = 0;
    for (bool field : undo_log.modified_fields_) {
      if (!field) {
        Value cur_val = tuple_next.GetValue(&schema, i);
        Value next_val = new_tuple.GetValue(&schema, i);
        if (!cur_val.CompareExactlyEquals(next_val)) {
          diff_values.emplace_back(cur_val);
          diff_columns.emplace_back(schema.GetColumn(i));
        }
        new_undo_log.modified_fields_.emplace_back(!cur_val.CompareExactlyEquals(next_val));
      } else {
        diff_values.emplace_back(undo_log.tuple_.GetValue(&prev_log_schema, log_tuple_iterator));
        diff_columns.emplace_back(schema.GetColumn(i));
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
    new_undo_log.is_deleted_ = cur_tuple_meta.is_deleted_;
    std::vector<Value> diff_values;
    std::vector<Column> diff_columns;
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      Value cur_val = tuple_next.GetValue(&schema, i);
      Value next_val = new_tuple.GetValue(&schema, i);
      if (!cur_val.CompareExactlyEquals(next_val)) {
        diff_values.emplace_back(cur_val);
        diff_columns.emplace_back(schema.GetColumn(i));
      }
      new_undo_log.modified_fields_.emplace_back(!cur_val.CompareExactlyEquals(next_val));
    }
    Schema diff_schema = Schema(diff_columns);
    new_undo_log.tuple_ = Tuple(diff_values, &diff_schema);
    new_undo_log.ts_ = ts;
    new_undo_log.prev_version_ = undo_link;
  }
  return new_undo_log;
}

auto GetUndoLogSchema(const UndoLog &undo_log, const Schema &schema) -> Schema {
  // generate schema for the previous log
  // so that we can access the previous tuple state
  std::vector<Column> prev_log_columns;
  int col_id = 0;
  for (bool field : undo_log.modified_fields_) {
    if (field) {
      prev_log_columns.emplace_back(schema.GetColumn(col_id));
    }
    col_id++;
  }
  Schema prev_log_schema(prev_log_columns);
  return prev_log_schema;
}

}  // namespace bustub
