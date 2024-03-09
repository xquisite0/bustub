//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "binder/bound_order_by.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys_;

  auto CustomComparator(const Tuple &a, const Tuple &b) -> bool {
    for (auto &p : order_bys_) {
      OrderByType type = p.first;
      AbstractExpressionRef expr = p.second;
      Value value_a = expr->Evaluate(&a, GetOutputSchema());
      Value value_b = expr->Evaluate(&b, GetOutputSchema());

      if (type == OrderByType::DEFAULT || type == OrderByType::ASC) {
        if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
          return true;
        }
        if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
          return false;
        }
        if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return true;
  }
};
}  // namespace bustub
