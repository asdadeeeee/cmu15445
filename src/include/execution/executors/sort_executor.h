//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  class Cmp {
   public:
    Cmp(std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, const Schema &schema)
        : order_bys_(std::move(order_bys)) {
      schema_ = std::make_shared<Schema>(schema);
    }
    auto operator()(const Tuple &A, const Tuple &B) -> bool {  // 从小到大
      for (const auto &order_pair : order_bys_) {
        auto value_a = order_pair.second->Evaluate(&A, *schema_);
        auto value_b = order_pair.second->Evaluate(&B, *schema_);
        if (value_a.CompareEquals(value_b) == CmpBool::CmpTrue) {
          continue;
        }
        if (order_pair.first == OrderByType::ASC || order_pair.first == OrderByType::DEFAULT) {
          return value_a.CompareLessThan(value_b) == CmpBool::CmpTrue;
        }
        if (order_pair.first == OrderByType::DESC) {
          return value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue;
        }
      }
      return true;
    }

   public:
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
    SchemaRef schema_;
  };

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> child_tuples_;
  std::vector<Tuple>::iterator child_tuples_iter_ = child_tuples_.begin();

  bool if_init_ = false;
};
}  // namespace bustub
