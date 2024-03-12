#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      pq_(CustomComparator(this)) {}

void TopNExecutor::Init() {
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};
  while (child_executor_->Next(&tuple_next, &rid_next)) {
    pq_.push(tuple_next);
    if (pq_.size() > plan_->GetN()) {
      pq_.pop();
    }
  }
  while (!pq_.empty()) {
    output_.emplace_back(pq_.top());
    pq_.pop();
  }
  initialized_ = true;
  iterator_ = output_.size() - 1;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!initialized_) {
    Init();
  }
  if (iterator_ < 0) {
    return false;
  }
  *tuple = output_[iterator_];
  *rid = tuple->GetRid();
  iterator_--;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return 0; };

}  // namespace bustub

/* Todo
implement a pqueue with a custom comparator
*/