#include "execution/executors/window_function_executor.h"
#include <climits>
#include "catalog/column.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  Tuple tuple_next = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID rid_next = RID{INVALID_PAGE_ID, 0};
  while (child_executor_->Next(&tuple_next, &rid_next)) {
    tuples_.emplace_back(tuple_next);
  }
  if (!plan_->window_functions_.empty()) {
    SortTuples(tuples_);
  }
  std::vector<std::vector<Value>> output_values(tuples_.size(), std::vector<Value>(GetOutputSchema().GetColumnCount()));

  // iterate through the window functions, and process them one-by-one
  // window function: partition_by, order_by, functions, window_func_type
  bool non_agg_cols_done = false;

  for (auto &p : plan_->window_functions_) {
    ht_.clear();
    int window_func_index = p.first;
    WindowFunctionPlanNode::WindowFunction window_func = p.second;
    AbstractExpressionRef function = window_func.function_;
    WindowFunctionType type = window_func.type_;
    std::vector<AbstractExpressionRef> partition_by = window_func.partition_by_;
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by = window_func.order_by_;

    // work on this later
    std::vector<AggregateKey> key_order;
    for (Tuple &tuple : tuples_) {
      // Value v = function->Evaluate(&tuple, child_executor_->GetOutputSchema());

      // Make an aggregate key based on the current window aggregate's partition
      AggregateKey key = MakeAggregateKey(&tuple, partition_by);
      if (ht_.find(key) == ht_.end()) {
        key_order.emplace_back(key);
      }
      ht_[key].emplace_back(tuple);
    }

    std::vector<Value> prev_order_values;
    std::vector<Value> cur_order_values;
    // process the aggregation columns
    size_t row_counter = 0;

    for (AggregateKey &key : key_order) {
      std::pair<AggregateKey, std::vector<Tuple>> ht_p = {key, ht_[key]};
      // THIS IS NULL INIT IT PROPERLY
      Value rolling_v(TypeId::INTEGER);
      Value prev_val = Value(TypeId::INTEGER);
      InitializeValue(&rolling_v, type);
      // we are calculating a moving aggregate when order by exists
      if (!order_by.empty()) {
        for (Tuple &tuple : ht_p.second) {
          cur_order_values = {};
          for (auto &order_by_ele : order_by) {
            cur_order_values.emplace_back(order_by_ele.second->Evaluate(&tuple, GetOutputSchema()));
          }

          Value v = function->Evaluate(&tuple, child_executor_->GetOutputSchema());
          UpdateValue(&rolling_v, v, cur_order_values, prev_order_values, row_counter, type);
          output_values[row_counter][window_func_index] = rolling_v;
          if (!non_agg_cols_done) {
            int index = 0;
            for (const AbstractExpressionRef &expr : plan_->columns_) {
              auto col_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
              if (col_expr->GetColIdx() == UINT_MAX) {
                continue;
              }
              Value non_agg_v = expr->Evaluate(&tuple, GetOutputSchema());
              if (!non_agg_v.IsNull()) {
                output_values[row_counter][index] = non_agg_v;
              }
              index++;
            }
          }
          row_counter++;
          prev_order_values = {};
          for (Value &order_v : cur_order_values) {
            prev_order_values.emplace_back(order_v);
          }
        }
      } else {
        // we are not calculating a moving aggregate
        size_t initial_row_counter_val = row_counter;
        for (Tuple &tuple : ht_p.second) {
          Value v = function->Evaluate(&tuple, child_executor_->GetOutputSchema());
          UpdateValue(&rolling_v, v, cur_order_values, prev_order_values, row_counter, type);
          prev_val = v;
          if (!non_agg_cols_done) {
            int index = 0;
            for (const AbstractExpressionRef &expr : plan_->columns_) {
              auto col_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
              if (col_expr->GetColIdx() == UINT_MAX) {
                continue;
              }
              Value non_agg_v = expr->Evaluate(&tuple, GetOutputSchema());
              if (!non_agg_v.IsNull()) {
                output_values[row_counter][index] = non_agg_v;
              }
              index++;
            }
          }
          row_counter++;
          prev_val = v;
        }
        row_counter = initial_row_counter_val;
        for (; row_counter < initial_row_counter_val + ht_p.second.size(); row_counter++) {
          output_values[row_counter][window_func_index] = rolling_v;
        }
      }
    }

    // process the non-aggregation columns
    // for ()
    non_agg_cols_done = true;
  }
  for (std::vector<Value> &values : output_values) {
    Tuple tuple = Tuple(values, &GetOutputSchema());
    output_.emplace_back(tuple);
  }
  initialized_ = true;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!initialized_) {
    Init();
  }
  if (iterator_ >= output_.size()) {
    return false;
  }
  *tuple = output_[iterator_];
  *rid = tuple->GetRid();
  iterator_++;
  return true;
}
}  // namespace bustub
