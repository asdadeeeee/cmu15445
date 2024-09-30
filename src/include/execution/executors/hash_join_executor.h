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
#include <vector>
#include <unordered_map>
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"


/** AggregateKey represents a key in an aggregation operation */
namespace bustub {
struct JoinKey {
  /** The group-by values */
  std::vector<Value> join_values_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.join_values_.size(); i++) {
      if (join_values_[i].CompareEquals(other.join_values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.join_values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

class SimpleJoinHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  // explicit SimpleJoinHashTable(const std::vector<AbstractExpressionRef> &join_exprs);
  // join_exprs_(join_exprs){}
  SimpleJoinHashTable() { ht_.clear(); }

  void InsertRight(const JoinKey &join_key, Tuple &&right_tuple) {
    if (ht_.count(join_key) == 0) {
      std::vector<Tuple> temp_tuple;
      temp_tuple.emplace_back(std::move(right_tuple));
      ht_.insert({join_key, std::move(temp_tuple)});
    } else {
      ht_[join_key].emplace_back(std::move(right_tuple));
    }
  }

  auto GetRightTuples(const JoinKey &join_key) -> std::vector<Tuple> & { return ht_[join_key]; }

  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<JoinKey, std::vector<Tuple>>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const JoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const std::vector<Tuple> & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<JoinKey, std::vector<Tuple>>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<JoinKey, std::vector<Tuple>> ht_;
  /** The aggregate expressions that we have */
  // const std::vector<AbstractExpressionRef> &join_exprs_;
};

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
  /** @return The tuple as an JoinKey */
  auto MakeLeftJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, plan_->GetLeftPlan()->OutputSchema()));
    }
    return {keys};
  }

  auto MakeRightJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, plan_->GetRightPlan()->OutputSchema()));
    }
    return {keys};
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unique_ptr<SimpleJoinHashTable> aht_;
  SimpleJoinHashTable::Iterator aht_iterator_;
  std::vector<Tuple> hashed_tuples_;
  std::vector<Tuple>::iterator hashed_tuples_iter_ = hashed_tuples_.begin();
  bool if_init_ = false;
};

}  // namespace bustub
