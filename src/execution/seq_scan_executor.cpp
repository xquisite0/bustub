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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // obtain table info
  table_oid_t table_oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);

  // obtain table iterator and assign as an attribute
  table_iterator_ = std::make_shared<TableIterator>(table_info->table_->MakeIterator());
  // std::cout << "Initialised table " << table_info->name_ << "\n";
  // std::cout << "This table's first page id is " << table_info->table_->GetFirstPageId() << " \n";
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
    ++(*table_iterator_);

    if (tuple_meta.is_deleted_) {
      continue;
    }

    if (filter_predicate != nullptr) {
      // if the row fails the predicate, move on to the next tuple
      Value fulfills_predicate = filter_predicate->Evaluate(&tuple_meta_and_tuple.second, GetOutputSchema());
      if (fulfills_predicate.CompareEquals(Value(TypeId::BOOLEAN, 1)) == CmpBool::CmpFalse) {
        continue;
      }
    }

    // tuple is valid
    *tuple = tuple_meta_and_tuple.second;
    *rid = cur_rid;
    break;
  }
  return true;
}

}  // namespace bustub
