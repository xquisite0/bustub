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
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // std::cout << child_executor_ << "\n";
  // std::cout << "Hi!" << "\n";
}

void InsertExecutor::Init() {
  // std::cout << child_executor_->GetOutputSchema();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // ensures that Next() only returns true with number of inserted rows ONCE
  if (inserted_) {
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
    // insert tuple into table
    std::optional<RID> rid_of_inserted = table_info->table_->InsertTuple(
        tuple_meta, tuple_next, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_oid);
    tuples_added++;

    // update indices
    for (IndexInfo *index_info : table_indexes) {
      if (rid_of_inserted.has_value()) {
        index_info->index_->InsertEntry(
            tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            rid_of_inserted.value(), exec_ctx_->GetTransaction());
      }
    }

    // update write set of transaction
    if (rid_of_inserted.has_value()) {
      exec_ctx_->GetTransaction()->AppendWriteSet(table_oid, rid_of_inserted.value());
    }
  }

  std::vector<Value> values = {Value(TypeId::INTEGER, tuples_added)};

  *tuple = Tuple(values, &GetOutputSchema());
  inserted_ = true;
  // std::cout << "We added " << tuples_added << " tuples\n";
  return true;
}

}  // namespace bustub
