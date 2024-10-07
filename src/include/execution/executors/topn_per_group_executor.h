//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_per_group_executor.h
//
// Identification: src/include/execution/executors/topn_per_group_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_per_group_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/** AggregateValue represents a value for each of the running aggregates */
struct RankValue {
  /** The aggregate values */
  std::vector<Value> rank_vals_;
  auto operator==(const RankValue &other) const -> bool {
    for (uint32_t i = 0; i < other.rank_vals_.size(); i++) {
      if (rank_vals_[i].CompareEquals(other.rank_vals_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

class RankValueCmp {
 public:
  RankValueCmp() { order_bys_.clear(); }
  explicit RankValueCmp(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys) {
    std::vector<OrderByType> order_types;
    order_types.reserve(order_bys.size());
    for (const auto &order_pair : order_bys) {
      order_types.emplace_back(order_pair.first);
    }
    order_bys_ = std::move(order_types);
  }

  RankValueCmp(RankValueCmp &&other) = default;
  RankValueCmp(RankValueCmp const &other) noexcept = default;
  auto operator=(const RankValueCmp &other) -> RankValueCmp & = default;
  auto operator=(RankValueCmp &&other) noexcept -> RankValueCmp & {
    if (this != &other) {
      order_bys_ = std::move(other.order_bys_);
    }
    return *this;
  }

  auto operator()(const RankValue &A, const RankValue &B) const -> bool {  // 从小到大
    for (size_t i = 0; i < order_bys_.size(); i++) {
      const auto &order_type = order_bys_[i];
      auto value_a = A.rank_vals_[i];
      auto value_b = B.rank_vals_[i];
      if (value_a.CompareEquals(value_b) == CmpBool::CmpTrue) {
        continue;
      }
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return value_a.CompareLessThan(value_b) == CmpBool::CmpTrue;
      }
      if (order_type == OrderByType::DESC) {
        return value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue;
      }
    }
    return false;
  }

 public:
  std::vector<OrderByType> order_bys_;
};

struct RankInfo {
  size_t count_;       // 当前值的出现次数
  size_t less_count_;  // 小于当前值的所有元素的数量
};

class RankTracker {
 public:
  explicit RankTracker(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys) {
    RankValueCmp cmp(order_bys);
    tree_ = std::make_unique<std::map<RankValue, RankInfo, RankValueCmp>>(cmp);
  }

 public:
  void Insert(const RankValue &value) {
    // auto it =  std::lower_bound(tree_.begin(),tree_.end(),value);
    auto it = tree_->lower_bound(value);

    // 更新当前值的信息
    if (it != tree_->end() && it->first == value) {
      it->second.count_ += 1;
    } else {
      size_t less_count = 0;
      if (it != tree_->begin()) {
        auto prev = std::prev(it);
        less_count = prev->second.less_count_ + prev->second.count_;
      }
      tree_->insert({value, {1, less_count}});
    }

    // 更新后续节点的 less_count
    if (it != tree_->end() && it->first == value) {
      it++;
    }
    while (it != tree_->end()) {
      it->second.less_count_ += 1;
      ++it;
    }

    total_elements_++;
  }

  void Remove(const RankValue &value) {
    auto it = tree_->find(value);
    if (it == tree_->end()) {
      return;  // 值不存在
    }

    // 减少计数或移除节点
    if (it->second.count_ > 1) {
      it->second.count_ -= 1;
      it++;
    } else {
      tree_->erase(it++);
    }

    // 更新后续节点的 less_count
    while (it != tree_->end()) {
      it->second.less_count_ -= 1;
      ++it;
    }

    total_elements_--;
  }

  auto GetRank(const RankValue &value) -> int {
    auto it = tree_->find(value);
    if (it == tree_->end()) {
      return -1;  // 值不存在
    }

    return it->second.less_count_ + 1;
  }

  auto GetMostRank() -> size_t {
    if (tree_->empty()) {
      return 0;
    }
    auto it = tree_->end();
    it--;
    return it->second.less_count_ + 1;
  }

 private:
  std::unique_ptr<std::map<RankValue, RankInfo, RankValueCmp>> tree_;
  size_t total_elements_ = 0;  // 维护总元素数以便快速更新 less_count
};

}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::RankValue> {
  auto operator()(const bustub::RankValue &rank_val) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &val : rank_val.rank_vals_) {
      if (!val.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * The TopNPerGroupExecutor executor executes a topn.
 */
class TopNPerGroupExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNPerGroupExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopNPerGroup plan to be executed
   */
  TopNPerGroupExecutor(ExecutorContext *exec_ctx, const TopNPerGroupPlanNode *plan,
                       std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopNPerGroup */
  void Init() override;

  /**
   * Yield the next tuple from the TopNPerGroup.
   * @param[out] tuple The next tuple produced by the TopNPerGroup
   * @param[out] rid The next tuple RID produced by the TopNPerGroup
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->GetGroupBy()) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  auto MakeRankValue(const Tuple *tuple) -> RankValue {
    std::vector<Value> values;
    for (const auto &expr : plan_->order_bys_) {
      values.emplace_back(expr.second->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {values};
  }

  auto InsertEntries(Tuple &&tuple) -> void {
    auto key = MakeAggregateKey(&tuple);
    auto val = MakeRankValue(&tuple);
    if (top_entries_.count(key) == 0) {
      top_entries_.insert({key, std::priority_queue<Tuple, std::vector<Tuple>, SortExecutor::Cmp>(cmp_)});
      auto tracker = std::make_unique<RankTracker>(cmp_.order_bys_);
      rank_trackers_.emplace(key, std::move(tracker));
    }
    if (rank_trackers_.at(key)->GetMostRank() <= plan_->GetN()) {
      top_entries_.at(key).push(std::move(tuple));
      rank_trackers_.at(key)->Insert(val);
    }
    while (rank_trackers_.at(key)->GetMostRank() > plan_->GetN()) {
      PopOneEntry(key);
    }
  }

  auto PopOneEntry(const AggregateKey &key) -> void {
    if (!top_entries_.at(key).empty()) {
      rank_trackers_.at(key)->Remove(MakeRankValue(&top_entries_.at(key).top()));
      top_entries_.at(key).pop();
    }
  }

 private:
  /** The TopNPerGroup plan node to be executed */
  const TopNPerGroupPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::unordered_map<AggregateKey, std::priority_queue<Tuple, std::vector<Tuple>, SortExecutor::Cmp>> top_entries_{};
  std::unordered_map<AggregateKey, std::unique_ptr<RankTracker>> rank_trackers_;
  // std::unordered_map<AggregateKey, RankValue> top_value_{};
  // std::unordered_map<AggregateKey, std::unordered_set<RankValue>> diff_value_{};
  // std::unordered_map<AggregateKey, size_t> curr_k_{};

  std::unordered_map<AggregateKey, std::vector<Tuple>> top_tuples_{};

  std::unordered_map<AggregateKey, std::vector<Tuple>>::iterator top_tuples_iter_ = top_tuples_.begin();
  std::vector<Tuple>::iterator tuples_iter_;
  SortExecutor::Cmp cmp_;

  bool if_init_ = false;
};

}  // namespace bustub
