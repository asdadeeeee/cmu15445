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
#include <cstddef>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // 避免递归init 此处易出BUG
  if (!if_init_) {
    if_init_ = true;
    left_executor_->Init();
    right_executor_->Init();
    Tuple left_tuple;
    RID left_rid;

    while (left_executor_->Next(&left_tuple, &left_rid)) {
      Tuple right_tuple;
      RID right_rid;
      bool have_right = false;
      right_executor_->Init();
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        auto value = plan_->Predicate()->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                                      plan_->GetRightPlan()->OutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          std::vector<Value> output_value;
          output_value.clear();
          output_value.reserve(GetOutputSchema().GetColumnCount());
          for (size_t col_idx = 0; col_idx < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); col_idx++) {
            output_value.emplace_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
          }
          for (size_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); col_idx++) {
            output_value.emplace_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_idx));
          }
          Tuple temp_output_tuple(output_value, &GetOutputSchema());
          nested_tuples_.emplace_back(std::move(temp_output_tuple));
          have_right = true;
        }
      }
      if (plan_->join_type_ == JoinType::LEFT && !have_right) {
        std::vector<Value> output_value;
        output_value.clear();
        output_value.reserve(GetOutputSchema().GetColumnCount());
        for (size_t col_idx = 0; col_idx < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); col_idx++) {
          output_value.emplace_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
        }
        for (size_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); col_idx++) {
          output_value.emplace_back(
              ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(col_idx).GetType()));
        }
        Tuple temp_output_tuple(output_value, &GetOutputSchema());
        nested_tuples_.emplace_back(std::move(temp_output_tuple));
      }
    }
  }
  nested_tuples_iter_ = nested_tuples_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (nested_tuples_iter_ != nested_tuples_.end()) {
    *tuple = Tuple(*nested_tuples_iter_);
    nested_tuples_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
