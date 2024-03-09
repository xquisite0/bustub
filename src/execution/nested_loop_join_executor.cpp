//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    // throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // std::vector<Tuple> inner_tuples;
  Schema left_schema = plan_->GetLeftPlan()->OutputSchema();
  Schema right_schema = plan_->GetRightPlan()->OutputSchema();
  // std::vector<Tuple> outer_tuples;
  Tuple inner_tuple = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID inner_rid = RID{INVALID_PAGE_ID, 0};

  // while (right_executor_->Next(&inner_tuple, &inner_rid)) {
  //   inner_tuples.emplace_back(inner_tuple);
  // }

  Tuple outer_tuple = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID outer_rid = RID{INVALID_PAGE_ID, 0};
  // while (left_executor_->Next(&outer_tuple, &outer_rid)) {
  //   outer_tuples.emplace_back(outer_tuple);
  // }
  while (left_executor_->Next(&outer_tuple, &outer_rid)) {
    right_executor_->Init();
    bool matched = false;
    // bool done = false;
    // std::cout << "Left: " << outer_tuple.ToString(&left_schema) << "\n";
    while (right_executor_->Next(&inner_tuple, &inner_rid)) {
      // if (!done) {
      //   // std::cout << "Right: " << inner_tuple.ToString(&right_schema) << "\n";
      // }
      Value fulfills_predicate =
          plan_->Predicate()->EvaluateJoin(&outer_tuple, left_schema, &inner_tuple, right_schema);

      // left & right tuples fulfill predicate
      if (fulfills_predicate.CompareEquals(Value(TypeId::BOOLEAN, 1)) == CmpBool::CmpTrue) {
        // extract the left tuple & right tuples contents and merge into one tuple
        std::vector<Value> values;
        uint32_t col_idx = 0;
        while (col_idx < left_schema.GetColumnCount()) {
          values.emplace_back(outer_tuple.GetValue(&left_schema, col_idx));
          col_idx++;
        }
        col_idx = 0;
        while (col_idx < right_schema.GetColumnCount()) {
          values.emplace_back(inner_tuple.GetValue(&right_schema, col_idx));
          col_idx++;
        }
        Tuple combined_tuple = Tuple(values, &GetOutputSchema());
        output_.emplace_back(combined_tuple);

        matched = true;
      }
    }
    // done = true;
    if (plan_->GetJoinType() == JoinType::LEFT && !matched) {
      std::vector<Value> values;
      uint32_t col_idx = 0;
      while (col_idx < left_schema.GetColumnCount()) {
        values.emplace_back(outer_tuple.GetValue(&left_schema, col_idx));
        col_idx++;
      }
      col_idx = 0;
      while (col_idx < right_schema.GetColumnCount()) {
        const Column &col = right_schema.GetColumn(col_idx);
        TypeId type_id = col.GetType();
        // std::cout << "Value created is null? " << val.IsNull() << "\n";
        values.emplace_back(ValueFactory::GetNullValueByType(type_id));
        col_idx++;
      }

      Tuple combined_tuple = Tuple(values, &GetOutputSchema());
      // std::cout << combined_tuple.ToString(&GetOutputSchema()) << "\n";
      output_.emplace_back(combined_tuple);
    }
  }
  iterator_ = 0;
  end_iterator_ = output_.size();
  initialized_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!initialized_) {
    Init();
  }

  if (iterator_ < end_iterator_) {
    *tuple = output_[iterator_];
    *rid = output_[iterator_].GetRid();
    iterator_++;
    return true;
  }
  return false;
}

}  // namespace bustub
