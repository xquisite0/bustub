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
#include "catalog/schema.h"
#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (exec_ctx != nullptr && exec_ctx->GetTransaction() != nullptr) {
    read_ts_ = exec_ctx->GetTransaction()->GetReadTs();
    temp_ts_ = exec_ctx->GetTransaction()->GetTransactionTempTs();
  }
}

void SeqScanExecutor::Init() {
  // obtain table info
  table_oid_t table_oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);

  // obtain table iterator and assign as an attribute
  table_iterator_ = std::make_shared<TableIterator>(table_info->table_->MakeIterator());
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
    ++(*table_iterator_);

    // tuple is valid
    std::optional<Tuple> tuple_opt =
        GetTuple(tuple_meta_and_tuple, cur_rid, read_ts_, temp_ts_, exec_ctx_, GetOutputSchema(), filter_predicate);
    if (!tuple_opt.has_value()) {
      continue;
    }
    *tuple = tuple_opt.value();
    *rid = cur_rid;
    break;
  }
  return true;
}

}  // namespace bustub
