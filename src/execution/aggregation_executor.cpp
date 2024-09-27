//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(std::make_unique<SimpleAggregationHashTable>(plan->aggregates_, plan->agg_types_)),
      aht_iterator_(aht_->Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple agg_tuple;
  RID agg_rid;
  aht_->Clear();
  if (plan_->group_bys_.empty()) {
    AggregateKey agg_key;
    aht_->InitInsert(agg_key);
  }
  while (child_executor_->Next(&agg_tuple, &agg_rid)) {
    AggregateKey agg_key = MakeAggregateKey(&agg_tuple);
    AggregateValue agg_value = MakeAggregateValue(&agg_tuple);
    aht_->InsertCombine(agg_key, agg_value);
  }
  aht_iterator_ = aht_->Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (aht_iterator_ != aht_->End()) {
    std::vector<Value> output_value;
    output_value.reserve(aht_iterator_.Key().group_bys_.size() + aht_iterator_.Val().aggregates_.size());
    output_value.insert(output_value.end(), aht_iterator_.Key().group_bys_.begin(),
                        aht_iterator_.Key().group_bys_.end());
    output_value.insert(output_value.end(), aht_iterator_.Val().aggregates_.begin(),
                        aht_iterator_.Val().aggregates_.end());
    *tuple = Tuple(output_value, &GetOutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
