//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    // throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  Schema left_schema = plan_->GetLeftPlan()->OutputSchema();
  Schema right_schema = plan_->GetRightPlan()->OutputSchema();

  Tuple inner_tuple = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID inner_rid = RID{INVALID_PAGE_ID, 0};

  // checks if the tuple has been matched

  Tuple outer_tuple = Tuple{RID{INVALID_PAGE_ID, 0}};
  RID outer_rid = RID{INVALID_PAGE_ID, 0};

  std::unordered_map<HashKey, bool> hash_matched;
  // build hash table by populating it with outer table tuples
  while (left_child_->Next(&outer_tuple, &outer_rid)) {
    HashKey left_hash = MakeLeftAggregateKey(&outer_tuple);

    // deep copy outer_tuple into hash table
    if (ht_.find(left_hash) != ht_.end()) {
      ht_[left_hash].emplace_back(outer_tuple);
    } else {
      ht_.insert({left_hash, {outer_tuple}});
    }
  }

  while (right_child_->Next(&inner_tuple, &inner_rid)) {
    HashKey right_hash = MakeRightAggregateKey(&inner_tuple);

    if (ht_.find(right_hash) == ht_.end()) {
      continue;
    }
    hash_matched[right_hash] = true;
    for (Tuple &cur_tuple : ht_[right_hash]) {
      std::vector<Value> values;
      uint32_t col_idx = 0;
      while (col_idx < left_schema.GetColumnCount()) {
        values.emplace_back(cur_tuple.GetValue(&left_schema, col_idx));
        col_idx++;
      }
      col_idx = 0;
      while (col_idx < right_schema.GetColumnCount()) {
        values.emplace_back(inner_tuple.GetValue(&right_schema, col_idx));
        col_idx++;
      }
      Tuple combined_tuple = Tuple(values, &GetOutputSchema());
      output_.emplace_back(combined_tuple);
    }
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    for (auto &p : ht_) {
      HashKey hk = p.first;
      if (!hash_matched[hk]) {
        for (Tuple &cur_tuple : p.second) {
          std::vector<Value> values;
          uint32_t col_idx = 0;
          while (col_idx < left_schema.GetColumnCount()) {
            values.emplace_back(cur_tuple.GetValue(&left_schema, col_idx));
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
          output_.emplace_back(combined_tuple);
        }
      }
    }
  }
  initialized_ = true;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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

/* Todo:
1. Create a helper function to hash tuples
2. Create a dynamic hash table
3. Implement the "Build" phase with linear probing
4. Implement the "Probe" phase.
*/