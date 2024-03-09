#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto VerifyEquiConditions(AbstractExpression *expr, std::vector<ComparisonExpression *> &comp_exprs) -> bool {
  if (expr == nullptr) {
    return false;
  }
  auto *expr_cast = dynamic_cast<ComparisonExpression *>(expr);
  if (expr_cast != nullptr) {
    if (expr_cast->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    comp_exprs.emplace_back(expr_cast);
    return true;
  }
  if (expr->children_.size() != 2) {
    return false;
  }
  if (expr->GetChildAt(0) != nullptr && expr->GetChildAt(1) != nullptr) {
    return VerifyEquiConditions(expr->GetChildAt(0).get(), comp_exprs) &&
           VerifyEquiConditions(expr->GetChildAt(1).get(), comp_exprs);
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    // auto merged_filter_nlj = OptimizeMergeFilterNLJ(child);
    children.emplace_back(OptimizeNLJAsHashJoin(child));
    // children.emplace_back(OptimizeMergeFilterNLJ(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  std::vector<AbstractExpressionRef> left_key_expressions;
  std::vector<AbstractExpressionRef> right_key_expressions;
  std::vector<ComparisonExpression *> comp_exprs;

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is 1 or more equi-conditions
    if (VerifyEquiConditions(nlj_plan.Predicate().get(), comp_exprs)) {
      // bool works = true;

      for (const auto *expr : comp_exprs) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            // Ensure both exprs have tuple_id == 0
            auto left_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
            auto right_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());

            if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
              left_key_expressions.emplace_back(left_expr_tuple_0);
              right_key_expressions.emplace_back(right_expr_tuple_0);
            }
            if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
              left_key_expressions.emplace_back(right_expr_tuple_0);
              right_key_expressions.emplace_back(left_expr_tuple_0);
            }
          }
        }
      }
    }
    if (left_key_expressions.size() == comp_exprs.size()) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                                nlj_plan.GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
/* Todo:
reference nlj_as_index_join.cpp

Hint: Make sure to check which table the column belongs to for each side of the equi-condition. It is possible that the
column from outer table is on the right side of the equi-condition. You may find ColumnValueExpression::GetTupleIdx
helpful.

Hint: The order to apply optimizer rules matters. For example, you want to optimize NestedLoopJoin into HashJoin after
filters and NestedLoopJoin have merged.

Hint When dealing with multiple equi-conditions, try to extract out the keys recursively, instead of matching the
joining condition with multiple layers of if clauses.

*/
