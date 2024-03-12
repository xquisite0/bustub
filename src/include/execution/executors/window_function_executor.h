//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> tuples_;
  auto CustomComparator(const Tuple &a, const Tuple &b) -> bool {
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys;
    for (auto &p : plan_->window_functions_) {
      order_bys = p.second.order_by_;
      break;
    }
    for (auto &p : order_bys) {
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
    return false;
  }
  void SortTuples(std::vector<Tuple> &tuples) {
    std::sort(tuples.begin(), tuples.end(), [this](const Tuple &a, const Tuple &b) { return CustomComparator(a, b); });
  }

  auto MakeAggregateKey(const Tuple *tuple, std::vector<AbstractExpressionRef> &partition_by) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_by.size());
    for (const auto &expr : partition_by) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  auto InitializeValue(Value *rolling_v, WindowFunctionType type) {
    switch (type) {
      case WindowFunctionType::CountStarAggregate:
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::Rank:
        *rolling_v = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // assumes all aggreations occur on integer types
        *rolling_v = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
    }
  }

  auto UpdateValue(Value *rolling_v, Value &cur_val, std::vector<Value> &cur_order_values,
                   std::vector<Value> &prev_order_values, size_t &row_counter, WindowFunctionType type) {
    bool tie = true;
    switch (type) {
      case WindowFunctionType::CountStarAggregate:
        *rolling_v = rolling_v->Add(ValueFactory::GetIntegerValue(1));
        break;
      case WindowFunctionType::CountAggregate:
        if (!cur_val.IsNull()) {
          if (!rolling_v->IsNull()) {
            *rolling_v = rolling_v->Add(ValueFactory::GetIntegerValue(1));
          } else {
            *rolling_v = ValueFactory::GetIntegerValue(1);
          }
        }
        break;
      case WindowFunctionType::Rank:
        // bool tie = true;
        if (cur_order_values.size() != prev_order_values.size()) {
          tie = false;
        } else {
          for (size_t i = 0; i < cur_order_values.size(); i++) {
            if (cur_order_values[i].CompareEquals(prev_order_values[i]) == CmpBool::CmpFalse) {
              tie = false;
              break;
            }
          }
        }

        if (!tie) {
          *rolling_v = ValueFactory::GetIntegerValue(row_counter + 1);
        }
        // if they are equal, rank will stay the same
        break;
      case WindowFunctionType::SumAggregate:
        if (!cur_val.IsNull()) {
          if (!rolling_v->IsNull()) {
            *rolling_v = rolling_v->Add(cur_val);
          } else {
            *rolling_v = ValueFactory::GetIntegerValue(0);
            *rolling_v = rolling_v->Add(cur_val);
          }
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (!cur_val.IsNull()) {
          if (!rolling_v->IsNull()) {
            *rolling_v = rolling_v->Min(cur_val);
          } else {
            *rolling_v = ValueFactory::GetIntegerValue(INT_MAX);
            *rolling_v = rolling_v->Min(cur_val);
          }
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (!cur_val.IsNull()) {
          if (!rolling_v->IsNull()) {
            *rolling_v = rolling_v->Max(cur_val);
          } else {
            *rolling_v = ValueFactory::GetIntegerValue(INT_MIN + 1);
            *rolling_v = rolling_v->Max(cur_val);
          }
        }
        break;
    }
  }

  bool initialized_{false};
  std::unordered_map<AggregateKey, std::vector<Tuple>> ht_;
  std::vector<Tuple> output_;
  size_t iterator_{0};
};
}  // namespace bustub
