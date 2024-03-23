#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

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

void Helper(TransactionManager *txn_mgr, RID cur_rid, const TableInfo *table_info) {
  // UndoLink undo_link;
  std::optional<UndoLink> undo_link_opt = txn_mgr->GetUndoLink(cur_rid);

  if (!undo_link_opt.has_value() || !undo_link_opt->IsValid()) {
    return;
  }
  UndoLink undo_link = *undo_link_opt;
  while (undo_link.IsValid()) {
    std::cout << "\t";
    std::cout << "txn" << undo_link.prev_txn_ - TXN_START_ID << "@" << undo_link.prev_log_idx_ << " ";
    UndoLog undo_log = txn_mgr->GetUndoLog(undo_link);
    std::cout << undo_log.tuple_.ToString(&table_info->schema_) << " ";
    std::cout << "ts=" << undo_log.ts_ << "\n";
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

    Helper(txn_mgr, cur_rid, table_info);
    ++iterator;
  }
}

}  // namespace bustub
