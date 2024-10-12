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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"
namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleWindowHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  explicit SimpleWindowHashTable(  // const AbstractExpressionRef window_expr,
      const WindowFunctionType &window_type)
      :  // window_expr_{window_expr},
        window_type_{window_type} {}

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue() -> Value {
    Value value;
    switch (window_type_) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        value = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::Rank:
        // Others starts at null.
        value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
    }
    return value;
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(const AggregateKey &key, const Value &input) {
    switch (window_type_) {
      case WindowFunctionType::CountStarAggregate: {
        ht_[key] = ht_[key].Add(input);
        break;
      }
      case WindowFunctionType::Rank: {
        if (!input.IsNull()) {
          if (intend_rank_.count(key) == 0) {
            Value v = ValueFactory::GetIntegerValue(1);
            intend_rank_.insert({key, v});
            ht_[key] = intend_rank_.at(key);
            curr_rank_value_.insert({key, input});
          } else {
            if (curr_rank_value_.at(key).CompareEquals(input) == CmpBool::CmpTrue ||
                curr_rank_value_.at(key).CompareEquals(input) == CmpBool::CmpFalse) {
              Value v1 = ValueFactory::GetIntegerValue(1);
              intend_rank_[key] = intend_rank_[key].Add(v1);
            }
            if (curr_rank_value_.at(key).CompareEquals(input) == CmpBool::CmpFalse) {
              ht_[key] = intend_rank_.at(key);
              curr_rank_value_[key] = input;
            }
          }
        }
        break;
      }
      case WindowFunctionType::CountAggregate: {
        if (!input.IsNull()) {
          if (ht_[key].IsNull()) {
            ht_[key] = ValueFactory::GetIntegerValue(1);
          } else {
            Value v1 = ValueFactory::GetIntegerValue(1);
            ht_[key] = ht_[key].Add(v1);
          }
        }
        break;
      }
      case WindowFunctionType::SumAggregate: {
        if (!input.IsNull()) {
          if (ht_[key].IsNull()) {
            ht_[key] = input;
          } else {
            ht_[key] = ht_[key].Add(input);
          }
        }
        break;
      }
      case WindowFunctionType::MinAggregate: {
        if (!input.IsNull()) {
          if (ht_[key].IsNull()) {
            ht_[key] = input;
          } else {
            ht_[key] = ht_[key].Min(input);
          }
        }
        break;
      }
      case WindowFunctionType::MaxAggregate: {
        if (!input.IsNull()) {
          if (ht_[key].IsNull()) {
            ht_[key] = input;
          } else {
            ht_[key] = ht_[key].Max(input);
          }
        }
        break;
      }
      default:
        break;
    }
    // *result = values;
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const Value &val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }

    CombineAggregateValues(agg_key, val);
  }

  void InitInsert(const AggregateKey &agg_key) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
  }

  auto GetValue(const AggregateKey &agg_key) const -> std::optional<Value> {
    if (ht_.count(agg_key) > 0) {
      return ht_.at(agg_key);
    }
    return std::nullopt;
  }

  /**
   * Clear the hash table
   */
  void Clear() {
    ht_.clear();
    curr_rank_value_.clear();
    intend_rank_.clear();
  }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, Value>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const Value & { return iter_->second; }

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
    std::unordered_map<AggregateKey, Value>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, Value> ht_{};
  /** The aggregate expressions that we have */
  // const AbstractExpressionRef &window_expr_;
  /** The types of aggregations that we have */
  const WindowFunctionType &window_type_;

  std::unordered_map<AggregateKey, Value> curr_rank_value_{};
  std::unordered_map<AggregateKey, Value> intend_rank_{};
  // Value curr_rank_value_;
  // Value intend_rank_;
};

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
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple, uint32_t window_func_idx) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->window_functions_.at(window_func_idx).partition_by_) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }
  auto MakeValue(const Tuple *tuple, uint32_t window_func_idx) -> Value {
    if (plan_->window_functions_.at(window_func_idx).type_ == WindowFunctionType::Rank) {
      return plan_->window_functions_.at(window_func_idx)
          .order_by_[0]
          .second->Evaluate(tuple, child_executor_->GetOutputSchema());
    }
    return plan_->window_functions_.at(window_func_idx).function_->Evaluate(tuple, child_executor_->GetOutputSchema());
  }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::unordered_map<uint32_t, std::unique_ptr<SimpleWindowHashTable>> hts_;
  std::optional<uint32_t> order_by_func_idx_ = std::nullopt;

  std::vector<Tuple> child_tuples_;
  std::vector<Tuple>::iterator child_tuples_iter_ = child_tuples_.begin();

  std::vector<Tuple> result_tuples_;
  std::vector<Tuple>::iterator result_tuples_iter_ = result_tuples_.begin();
  bool if_init_ = false;
};
}  // namespace bustub
