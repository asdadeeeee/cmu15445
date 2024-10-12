//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_per_group_executor.cpp
//
// Identification: src/execution/topn_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_per_group_executor.h"
#include "common/rid.h"
#include "execution/expressions/column_value_expression.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

TopNPerGroupExecutor::TopNPerGroupExecutor(ExecutorContext *exec_ctx, const TopNPerGroupPlanNode *plan,
                                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      cmp_(plan_->GetOrderBy(), child_executor_->GetOutputSchema()) {}

void TopNPerGroupExecutor::Init() {
  if (!if_init_) {
    if_init_ = true;
    child_executor_->Init();
    Tuple child_tuple;
    RID child_rid;

    while (child_executor_->Next(&child_tuple, &child_rid)) {
      InsertEntries(std::move(child_tuple));
    }
    // 可优化 多线程
    for (const auto &top_entry : top_entries_) {
      while (!top_entry.second.empty()) {
        std::vector<Value> result_value;
        result_value.clear();
        result_value.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t col_idx = 0; col_idx < GetOutputSchema().GetColumnCount(); col_idx++) {
          const auto &output_column = plan_->columns_[col_idx];
          const auto &col_val = dynamic_cast<const ColumnValueExpression &>(*output_column);
          if (col_val.GetColIdx() == static_cast<uint32_t>(-1)) {
            auto v = ValueFactory::GetIntegerValue(rank_trackers_.at(top_entry.first)->GetMostRank());
            result_value.emplace_back(std::move(v));
          } else {
            result_value.emplace_back(
                top_entry.second.top().GetValue(&child_executor_->GetOutputSchema(), col_val.GetColIdx()));
          }
        }
        PopOneEntry(top_entry.first);
        Tuple result_tuple(result_value, &GetOutputSchema());
        if (top_tuples_.count(top_entry.first) == 0) {
          top_tuples_.insert({top_entry.first, std::vector<Tuple>()});
        }
        // 头插
        top_tuples_.at(top_entry.first).insert(top_tuples_.at(top_entry.first).begin(), std::move(result_tuple));
      }
    }
  }
  top_tuples_iter_ = top_tuples_.begin();
  if (top_tuples_iter_ != top_tuples_.end()) {
    tuples_iter_ = top_tuples_iter_->second.begin();
  }
}

auto TopNPerGroupExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (top_tuples_iter_ != top_tuples_.end()) {
    while (tuples_iter_ != top_tuples_iter_->second.end()) {
      *tuple = Tuple(*tuples_iter_);
      tuples_iter_++;
      return true;
    }
    top_tuples_iter_++;
    if (top_tuples_iter_ != top_tuples_.end()) {
      tuples_iter_ = top_tuples_iter_->second.begin();
    }
  }
  return false;
}

}  // namespace bustub
