//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/config.h"
#include "common/rid.h"
#include "type/value_factory.h"
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  index_info_ = catalog->GetIndex(plan_->index_oid_);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  std::vector<Value> key_value_vec;
  key_value_vec.reserve(index_info_->index_->GetIndexColumnCount());
  // 本来应该这样的 2023fall的问题
    //   for (auto key_value_expr : plan_->pred_key_) {
    //     Value key_value = key_value_expr->val_;
    //     key_value_vec.emplace_back(key_value);
    //   }
  key_value_vec.emplace_back(plan_->pred_key_->val_);
  Tuple key(key_value_vec, &index_info_->key_schema_);
  htable_->ScanKey(key, &rids_, exec_ctx_->GetTransaction());
  index_iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (index_iter_ != rids_.end()) {
    auto tuple_pair = table_info_->table_->GetTuple(*index_iter_);
    if (tuple_pair.first.is_deleted_) {
      index_iter_++;
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&tuple_pair.second, GetOutputSchema());
      if (!value.IsNull() && !value.GetAs<bool>()) {
        index_iter_++;
        continue;
      }
    }
    *tuple = Tuple(tuple_pair.second);
    *rid = RID(index_iter_->GetPageId(), index_iter_->GetSlotNum());
    index_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
