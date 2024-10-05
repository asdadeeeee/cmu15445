#include "execution/executors/window_function_executor.h"
#include <cstdint>
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  for (const auto &window_function : plan_->window_functions_) {
    auto window_table = std::make_unique<SimpleWindowHashTable>(window_function.second.type_);
    auto iter = window_table->Begin();
    hts_.insert({window_function.first, std::move(window_table)});
    iters_.insert({window_function.first, iter});
    if (!window_function.second.order_by_.empty() && !order_by_func_idx_.has_value()) {
      order_by_func_idx_ = window_function.first;
    }
  }
}
void WindowFunctionExecutor::Init() {
  if (!if_init_) {
    if_init_ = true;
    child_executor_->Init();
    Tuple child_tuple;
    RID child_rid;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      child_tuples_.emplace_back(std::move(child_tuple));
    }
    for (auto &ht : hts_) {
      ht.second->Clear();
    }
    if (order_by_func_idx_.has_value()) {
      SortExecutor::Cmp cmp(plan_->window_functions_.at(order_by_func_idx_.value()).order_by_,
                            child_executor_->GetOutputSchema());
      std::sort(child_tuples_.begin(), child_tuples_.end(), cmp);
    }
    child_tuples_iter_ = child_tuples_.begin();
    if (order_by_func_idx_.has_value()) {
      while (child_tuples_iter_ != child_tuples_.end()) {
        std::vector<Value> result_value;
        result_value.clear();
        result_value.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t col_idx = 0; col_idx < GetOutputSchema().GetColumnCount(); col_idx++) {
          const auto &output_column = plan_->columns_[col_idx];
          const auto &col_val = dynamic_cast<const ColumnValueExpression &>(*output_column);
          if (col_val.GetColIdx() == static_cast<uint32_t>(-1)) {
            const auto &window_function = plan_->window_functions_.at(col_idx);
            if (window_function.partition_by_.empty()) {
              AggregateKey agg_key;
              hts_[col_idx]->InitInsert(agg_key);
            }
            auto key = MakeAggregateKey(&*child_tuples_iter_, col_idx);
            auto val = MakeValue(&*child_tuples_iter_, col_idx);
            hts_[col_idx]->InsertCombine(key, val);
            auto v = hts_[col_idx]->GetValue(key);
            if (v.has_value()) {
              result_value.emplace_back(std::move(v.value()));
            }
          } else {
            result_value.emplace_back(
                child_tuples_iter_->GetValue(&child_executor_->GetOutputSchema(), col_val.GetColIdx()));
          }
        }
        Tuple temp_result_tuple(result_value, &GetOutputSchema());
        result_tuples_.emplace_back(std::move(temp_result_tuple));
        child_tuples_iter_++;
      }
    } else {
      while (child_tuples_iter_ != child_tuples_.end()) {
        for (uint32_t col_idx = 0; col_idx < GetOutputSchema().GetColumnCount(); col_idx++) {
          const auto &output_column = plan_->columns_[col_idx];
          const auto &col_val = dynamic_cast<const ColumnValueExpression &>(*output_column);
          if (col_val.GetColIdx() == static_cast<uint32_t>(-1)) {
            const auto &window_function = plan_->window_functions_.at(col_idx);
            if (window_function.partition_by_.empty()) {
              AggregateKey agg_key;
              hts_[col_idx]->InitInsert(agg_key);
            }
            auto key = MakeAggregateKey(&*child_tuples_iter_, col_idx);
            auto val = MakeValue(&*child_tuples_iter_, col_idx);
            hts_[col_idx]->InsertCombine(key, val);
          }
        }
        child_tuples_iter_++;
      }
      child_tuples_iter_ = child_tuples_.begin();
      while (child_tuples_iter_ != child_tuples_.end()) {
        std::vector<Value> result_value;
        result_value.clear();
        result_value.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t col_idx = 0; col_idx < GetOutputSchema().GetColumnCount(); col_idx++) {
          const auto &output_column = plan_->columns_[col_idx];
          const auto &col_val = dynamic_cast<const ColumnValueExpression &>(*output_column);
          if (col_val.GetColIdx() == static_cast<uint32_t>(-1)) {
            auto key = MakeAggregateKey(&*child_tuples_iter_, col_idx);
            auto v = hts_[col_idx]->GetValue(key);
            if (v.has_value()) {
              result_value.emplace_back(std::move(v.value()));
            }
          } else {
            result_value.emplace_back(
                child_tuples_iter_->GetValue(&child_executor_->GetOutputSchema(), col_val.GetColIdx()));
          }
        }
        Tuple temp_result_tuple(result_value, &GetOutputSchema());
        result_tuples_.emplace_back(std::move(temp_result_tuple));
        child_tuples_iter_++;
      }
    }
  }
  result_tuples_iter_ = result_tuples_.begin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (result_tuples_iter_ != result_tuples_.end()) {
    *tuple = Tuple(*result_tuples_iter_);
    result_tuples_iter_++;
    return true;
  }
  return false;
}
}  // namespace bustub
