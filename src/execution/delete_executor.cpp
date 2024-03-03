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

  // initialise the tuple meta
  TupleMeta tuple_meta = {0, true};

  // obtain the list of table_indexes
  std::vector<IndexInfo *> table_indexes = catalog->GetTableIndexes(table_info->name_);

  // deletes tuples from the table + updates indexes

  int tuples_deleted = 0;
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};

  while (child_executor_->Next(&tuple_next, &rid_next)) {
    // delete tuple
    table_info->table_->UpdateTupleMeta(tuple_meta, rid_next);

    tuples_deleted++;

    // update indices
    for (IndexInfo *index_info : table_indexes) {
      Tuple old_key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, rid_next, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values = {Value(TypeId::INTEGER, tuples_deleted)};

  *tuple = Tuple(values, &GetOutputSchema());
  deleted_ = true;
  // std::cout << "We added " << tuples_added << " tuples\n";
  return true;
}

}  // namespace bustub
