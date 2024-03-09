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

  std::vector<Tuple> tuples;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.emplace_back(tuple);
  }
  // sort(tuples.begin(), tuples.end(), CustomComparator);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub

/* Todo

1. Create custom comparator
2. Sort, while accounting for ASC/DESC

*/
