#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "limit must have exactly one children");
    const auto &child = *optimized_plan->children_[0];
    if (child.GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(child);

      // SchemaRef output, AbstractPlanNodeRef child, std::vector<std::pair<OrderByType, AbstractExpressionRef>>
      // order_bys, std::size_t n
      SchemaRef output = std::make_shared<Schema>(optimized_plan->OutputSchema());
      AbstractPlanNodeRef child = sort_plan.children_[0];
      BUSTUB_ASSERT(sort_plan.children_.size() == 1, "topn must have exactly one children");
      auto order_bys = sort_plan.GetOrderBy();
      std::size_t n = limit_plan.GetLimit();

      return std::make_shared<TopNPlanNode>(output, child, order_bys, n);
    }
  }

  return optimized_plan;
}

}  // namespace bustub
