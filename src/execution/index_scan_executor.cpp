//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/config.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { emitted_ = false; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (emitted_) {
    return false;
  }
  // obtain table info
  table_oid_t table_oid = plan_->table_oid_;
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);

  // obtain index info
  index_oid_t index_oid = plan_->GetIndexOid();
  IndexInfo *index_info = catalog->GetIndex(index_oid);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  // obtain predicate info
  AbstractExpressionRef filter_predicate = plan_->filter_predicate_;
  const ConstantValueExpression *pred_key = plan_->pred_key_;

  // generate the key to use in htable
  // values passed in not important, CVEs will just return their values
  // the project will only use an index scan with one column as the predicate, thus values array is just one value.
  Tuple key = Tuple({pred_key->Evaluate(nullptr, GetOutputSchema())}, htable->GetKeySchema());

  std::vector<RID> result;
  // find the RIDs of tuples that match the predicate
  htable->ScanKey(key, &result, exec_ctx_->GetTransaction());
  bool tuple_found = false;
  // lookup tuples scanned in table heap
  for (RID &cur_rid : result) {

    // TODO (p4): iterate through the version chain to generate the tuple, refer to seq_scan_executor
    // maybe... make a helper function that is shared between the 2 scan executors?
    
    std::pair<TupleMeta, Tuple> tuplemeta_and_tuple = table_info->table_->GetTuple(cur_rid);

    // since the project assumes unique keys, we are only looking at one tuple, immediately assign this tuple to our
    // [out] tuple
    *tuple = tuplemeta_and_tuple.second;
    *rid = cur_rid;

    std::cout << "[IndexScan] We are emitting this tuple " << (*tuple).ToString(&GetOutputSchema()) << "\n";
    tuple_found = true;
  }
  emitted_ = true;
  return tuple_found;
}

}  // namespace bustub
