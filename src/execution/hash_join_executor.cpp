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
#include <cstdint>
#include <vector>
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      aht_(std::make_unique<SimpleJoinHashTable>()),
      aht_iterator_(aht_->Begin()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // 避免递归init 此处易出BUG
  if (!if_init_) {
    if_init_ = true;
    left_executor_->Init();
    right_executor_->Init();
    Tuple right_tuple;
    RID right_rid;

    while (right_executor_->Next(&right_tuple, &right_rid)) {
      JoinKey right_key = MakeRightJoinKey(&right_tuple);
      aht_->InsertRight(right_key, std::move(right_tuple));
    }
    Tuple left_tuple;
    RID left_rid;
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      bool have_right = false;
      JoinKey left_key = MakeLeftJoinKey(&left_tuple);
      std::vector<Value> left_value_vec;
      left_value_vec.clear();
      left_value_vec.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount());
      Value left_temp_value;
      for (uint32_t col_idx = 0; col_idx < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); col_idx++) {
        left_temp_value = left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx);
        left_value_vec.emplace_back(std::move(left_temp_value));
      }
      std::vector<Value> value_vec;
      for (const auto &right_tuple : aht_->GetRightTuples(left_key)) {
        have_right = true;
        value_vec.clear();
        value_vec.reserve(plan_->OutputSchema().GetColumnCount());
        value_vec.insert(value_vec.end(), left_value_vec.begin(), left_value_vec.end());
        Value temp_value;
        for (uint32_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); col_idx++) {
          temp_value = right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_idx);
          value_vec.emplace_back(std::move(temp_value));
        }
        Tuple temp_tuple(value_vec, &plan_->OutputSchema());
        hashed_tuples_.emplace_back(std::move(temp_tuple));
      }
      if (plan_->join_type_ == JoinType::LEFT && !have_right) {
        value_vec.clear();
        value_vec.reserve(plan_->OutputSchema().GetColumnCount());
        value_vec.insert(value_vec.end(), left_value_vec.begin(), left_value_vec.end());
        for (size_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); col_idx++) {
          value_vec.emplace_back(
              ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(col_idx).GetType()));
        }
        Tuple temp_tuple(value_vec, &plan_->OutputSchema());
        hashed_tuples_.emplace_back(std::move(temp_tuple));
      }
    }
  }
  hashed_tuples_iter_ = hashed_tuples_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (hashed_tuples_iter_ != hashed_tuples_.end()) {
    *tuple = Tuple(*hashed_tuples_iter_);
    hashed_tuples_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
