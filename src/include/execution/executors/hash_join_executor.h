//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
namespace bustub {
struct HashKey {
  /** The group-by values */
  std::vector<Value> attributes_;

  /**
   * Compares two hash keys for equality.
   * @param other the other hash key to be compared with
   * @return `true` if both hash keys have equivalent attribute expressions, `false` otherwise
   */
  auto operator==(const HashKey &other) const -> bool {
    for (uint32_t i = 0; i < other.attributes_.size(); i++) {
      if (attributes_[i].CompareEquals(other.attributes_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashValue {
  /** The values */
  std::vector<Value> values_;
};

}  // namespace bustub
namespace std {
template <>
struct hash<bustub::HashKey> {
  auto operator()(const bustub::HashKey &hash_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.attributes_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std
namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto MakeLeftAggregateKey(const Tuple *tuple) -> HashKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
    }
    return {keys};
  }
  auto MakeRightAggregateKey(const Tuple *tuple) -> HashKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
    }
    return {keys};
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unordered_map<HashKey, std::vector<Tuple>> ht_;
  std::vector<Tuple> output_;
  bool initialized_{false};
  uint32_t iterator_{0};
};

}  // namespace bustub
