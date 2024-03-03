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

  // initialise the tuple meta
  TupleMeta old_tuple_meta = {0, true};
  TupleMeta new_tuple_meta = {0, false};

  // obtain the list of table_indexes
  std::vector<IndexInfo *> table_indexes = catalog->GetTableIndexes(table_info->name_);

  // obtain the update expressions
  std::vector<AbstractExpressionRef> target_expressions = plan_->target_expressions_;

  // update tuples into the table + update indexes
  int tuples_updated = 0;
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};
  // std::cout << rid_next << "\n";

  while (child_executor_->Next(&tuple_next, &rid_next)) {
    // delete old tuple
    table_info->table_->UpdateTupleMeta(old_tuple_meta, rid_next);

    // update tuple columns
    std::vector<Value> new_values;
    new_values.reserve(target_expressions.size());
    for (AbstractExpressionRef &target_expression : target_expressions) {
      // std::cout << &GetOutputSchema() << "\n";
      new_values.push_back(target_expression->Evaluate(&tuple_next, child_executor_->GetOutputSchema()));
    }
    // std::cout << tuple_next.ToString(&child_executor_->GetOutputSchema()) << "\n";

    // insert tuple into table
    Tuple new_tuple = Tuple(new_values, &child_executor_->GetOutputSchema());

    std::optional<RID> rid_of_inserted = table_info->table_->InsertTuple(
        new_tuple_meta, new_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_oid);
    tuples_updated++;

    // update indices
    for (IndexInfo *index_info : table_indexes) {
      Tuple old_key =
          tuple_next.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      Tuple new_key =
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, rid_next, exec_ctx_->GetTransaction());
      if (rid_of_inserted.has_value()) {
        index_info->index_->InsertEntry(new_key, rid_of_inserted.value(), exec_ctx_->GetTransaction());
      }
    }
  }

  std::vector<Value> values = {Value(TypeId::INTEGER, tuples_updated)};

  *tuple = Tuple(values, &GetOutputSchema());
  updated_ = true;
  // std::cout << "We added " << tuples_added << " tuples\n";
  return true;
}

}  // namespace bustub
