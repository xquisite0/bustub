#include <algorithm>
#include <memory>
#include <optional>
#include <tuple>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    // BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");

    // check that there is a filter predicate with one row
    AbstractExpressionRef filter_predicate = seq_scan_plan.filter_predicate_;
    if (filter_predicate != nullptr) {
      // we have a predicate with just one equality test
      auto comparison_expression = dynamic_cast<ComparisonExpression *>(filter_predicate.get());
      if (comparison_expression != nullptr && comparison_expression->comp_type_ == ComparisonType::Equal) {
        // std::cout << (*filter_predicate).ToString() << " is a single column equality predicate!"
        // << "\n";x

        // let's check if there exists an index for this column
        const std::string &table_name = seq_scan_plan.table_name_;
        std::vector<IndexInfo *> table_indexes = catalog_.GetTableIndexes(table_name);

        for (IndexInfo *table_index : table_indexes) {
          // std::cout << table_index->index_->ToString() << "\n";
          // std::cout << "KeyAttrs: ";
          // for (uint32_t i : table_index->index_->GetKeyAttrs()) {
          //   std::cout << i << " ";
          // }
          // std::cout << "\n\n";
          const std::vector<uint32_t> key_attrs = table_index->index_->GetKeyAttrs();
          // index on 1 column only that matches our predicate column
          auto predicate_column_value_expression =
              dynamic_cast<ColumnValueExpression *>(comparison_expression->GetChildren()[0].get());

          if (predicate_column_value_expression == nullptr) {
            break;
          }
          uint32_t predicate_colid = predicate_column_value_expression->GetColIdx();
          if (key_attrs.size() == 1 && key_attrs[0] == predicate_colid) {
            auto pred_key = dynamic_cast<ConstantValueExpression *>(comparison_expression->GetChildren()[1].get());
            auto index_scan_plan_node =
                std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                    table_index->index_oid_, filter_predicate, pred_key);
            return index_scan_plan_node;
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
