#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      order_bys_(plan_->GetOrderBy()) {}

void SortExecutor::Init() {
  Tuple tuple = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid = RID{INVALID_PAGE_ID, 0};

  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  SortTuples(tuples_);
  initialized_ = true;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!initialized_) {
    Init();
  }
  if (iterator_ >= tuples_.size()) {
    return false;
  }
  *tuple = tuples_[iterator_];
  *rid = tuple->GetRid();
  iterator_++;
  return true;
}

}  // namespace bustub

/* Todo

1. Create custom comparator
2. Sort, while accounting for ASC/DESC

*/
