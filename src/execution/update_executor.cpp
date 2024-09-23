//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <memory>
#include <utility>

#include "execution/executors/update_executor.h"
#include "type/type.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  if_executed_ = false;
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t insert_table_oid = plan_->GetTableOid();
  table_info_ = catalog->GetTable(insert_table_oid);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (if_executed_) {
    return false;
  }
  if_executed_ = true;
  Catalog *catalog = exec_ctx_->GetCatalog();
  auto table_indexs = catalog->GetTableIndexes(table_info_->name_);
  Tuple update_tuple;
  RID update_old_rid;
  std::vector<std::pair<RID, RID>> temp_update_old_new_rids;
  while (child_executor_->Next(&update_tuple, &update_old_rid)) {
    // Compute expressions
    std::vector<Value> values{};
    values.reserve(table_info_->schema_.GetColumnCount());
    for (size_t i = 0; i < plan_->target_expressions_.size(); i++) {
      const auto &expr = plan_->target_expressions_[i];
      Value new_value = expr->Evaluate(&update_tuple, table_info_->schema_);
      if (new_value.IsNull() || new_value.GetTypeId() != table_info_->schema_.GetColumn(i).GetType()) {
        UndoUpdate(temp_update_old_new_rids);
        throw bustub::Exception("update table schema mismatch");
      }
      values.push_back(expr->Evaluate(&update_tuple, table_info_->schema_));
    }
    Tuple new_tupe(values, &table_info_->schema_);

    // 插新record
    TupleMeta insert_tuple_meta{INVALID_TS, false};
    auto update_new_rid = table_info_->table_->InsertTuple(insert_tuple_meta, new_tupe, exec_ctx_->GetLockManager(),
                                                           exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!update_new_rid.has_value()) {
      UndoUpdate(temp_update_old_new_rids);
      throw bustub::Exception("insert failed has no empty space");
    }
    temp_update_old_new_rids.emplace_back(update_old_rid, update_new_rid.value());

    // 删旧record
    TupleMeta update_tuple_meta{INVALID_TS, true};
    table_info_->table_->UpdateTupleMeta(update_tuple_meta, update_old_rid);

    for (auto &index : table_indexs) {
      // 删旧索引
      Tuple old_index_tuple =
          update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_index_tuple, update_old_rid, exec_ctx_->GetTransaction());

      // 插新索引
      Tuple new_index_tuple =
          new_tupe.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      if (!index->index_->InsertEntry(new_index_tuple, update_new_rid.value(), exec_ctx_->GetTransaction())) {
        UndoUpdate(temp_update_old_new_rids);
        throw bustub::Exception("insert failed duplicate index");
      }
    }
  }
  Value ret_value = ValueFactory::GetIntegerValue(temp_update_old_new_rids.size());
  std::vector<Value> ret_value_vec;
  ret_value_vec.reserve(GetOutputSchema().GetColumnCount());
  ret_value_vec.emplace_back(ret_value);
  *tuple = Tuple{ret_value_vec, &GetOutputSchema()};
  return true;
}

void UpdateExecutor::UndoUpdate(std::vector<std::pair<RID, RID>> &temp_update_old_new_rids) {
  Catalog *catalog = exec_ctx_->GetCatalog();
  auto table_indexs = catalog->GetTableIndexes(table_info_->name_);
  for (auto &old_new_rids : temp_update_old_new_rids) {
    // 恢复旧record
    TupleMeta old_tuple_meta{INVALID_TS, false};
    table_info_->table_->UpdateTupleMeta(old_tuple_meta, old_new_rids.first);

    // 删掉新record
    TupleMeta new_tuple_meta{INVALID_TS, true};
    table_info_->table_->UpdateTupleMeta(new_tuple_meta, old_new_rids.second);

    // 删掉新索引，插入旧索引
    auto old_tuple_pair = table_info_->table_->GetTuple(old_new_rids.first);
    auto new_tuple_pair = table_info_->table_->GetTuple(old_new_rids.second);
    for (auto &index : table_indexs) {
      Tuple new_tuple = new_tuple_pair.second;
      Tuple new_index_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(new_index_tuple, old_new_rids.second, exec_ctx_->GetTransaction());
      Tuple old_tuple = old_tuple_pair.second;
      Tuple old_index_tuple =
          old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());

      // 删一下避免 更新后的还未回退的其他索引占了该索引位 这个易出bug，此处rid参数无意义
      index->index_->DeleteEntry(old_index_tuple, old_new_rids.second, exec_ctx_->GetTransaction());
      bool insert_success =
          index->index_->InsertEntry(old_index_tuple, old_new_rids.first, exec_ctx_->GetTransaction());
      if (!insert_success) {
        throw bustub::Exception("should not be here");
      }
    }
  }
}

}  // namespace bustub
