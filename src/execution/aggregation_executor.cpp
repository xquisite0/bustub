//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};

  bool empty_table = true;
  while (child_executor_->Next(&tuple_next, &rid_next)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple_next), MakeAggregateValue(&tuple_next));

    empty_table = false;
  }

  if (empty_table && plan_->GetGroupBys().empty()) {
    aht_.EmptyTableInit();
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // populate hash table with values
  if (!built_) {
    Init();
  }
  built_ = true;

  // emit the tuples
  if (aht_iterator_ != aht_.End()) {
    // how to convert ze ht key & val to tuple?

    // result requires group by columns as well
    if (GetOutputSchema().GetColumnCount() > aht_iterator_.Val().aggregates_.size()) {
      std::vector<Value> keys = aht_iterator_.Key().group_bys_;
      std::vector<Value> values = aht_iterator_.Val().aggregates_;
      keys.insert(keys.end(), values.begin(), values.end());
      *tuple = Tuple(keys, &GetOutputSchema());
    } else {
      *tuple = Tuple(aht_iterator_.Val().aggregates_, &GetOutputSchema());
    }
    *rid = tuple->GetRid();
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
