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
#include <vector>

#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
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
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  table_indexs_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  child_tuples_.clear();
  temp_updated_rids_.clear();
  Tuple update_tuple;
  RID update_old_rid;
  while (child_executor_->Next(&update_tuple, &update_old_rid)) {
    child_tuples_.emplace_back(update_old_rid, update_tuple);
  }
}
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<std::pair<RID, RID>> temp_update_old_new_rids;
  if (if_executed_) {
    return false;
  }
  if_executed_ = true;
  auto child_iter = child_tuples_.begin();
  while (child_iter != child_tuples_.end()) {
    auto base_meta = table_info_->table_->GetTupleMeta(child_iter->first);
    // heap meta写写冲突
    if (!CanTupleBeSeen(base_meta.ts_, txn_)) {
      txn_->SetTainted();
      throw ExecutionException("Update write conflict base_meta");
    }

    // Compute expressions
    std::vector<Value> all_values{};
    all_values.reserve(table_info_->schema_.GetColumnCount());
    for (size_t i = 0; i < plan_->target_expressions_.size(); i++) {
      const auto &expr = plan_->target_expressions_[i];
      Value value = expr->Evaluate(&child_iter->second, table_info_->schema_);
      if (value.GetTypeId() != table_info_->schema_.GetColumn(i).GetType()) {
        // UndoUpdate(temp_update_old_new_rids);
        throw bustub::Exception("update table schema mismatch");
      }
      all_values.push_back(value);
    }
    Tuple new_tuple(all_values, &table_info_->schema_);

    // 如果ts不等 说明该txn_没有修改过这个tuple，那么需要重新构造undo并append updateundolink
    // 否则两种情况，
    // **TODO 一种是新增的uncommited tuple，这种也可能会有undolink （index insert）
    // 一种是uncommited updated tuple，这种必然有undolog且为最新的,并在该条undolog上修改 用ModifyUndoLog
    if (!IsTupleContentEqual(new_tuple, child_iter->second)) {
      if (base_meta.ts_ != txn_->GetTransactionTempTs()) {
        std::vector<Value> old_values;
        std::vector<bool> modified_fields(table_info_->schema_.GetColumnCount(), false);

        for (size_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
          Value old_value = child_iter->second.GetValue(&table_info_->schema_, i);
          Value new_value = new_tuple.GetValue(&table_info_->schema_, i);
          if (!old_value.CompareExactlyEquals(new_value) && !modified_fields.at(i)) {
            modified_fields.at(i) = true;
            old_values.emplace_back(std::move(old_value));
          }
        }

        const Schema *schema_ptr = &table_info_->schema_;
        auto undo_schema = ConstructUndoLogSchema(schema_ptr, modified_fields);
        Tuple undo_tuple{old_values, &undo_schema};
        auto pre_undo_link = txn_mgr_->GetUndoLink(child_iter->first);
        if (!(pre_undo_link.has_value() && pre_undo_link->IsValid())) {
          pre_undo_link = UndoLink{};
        }

        auto undo_link = txn_->AppendUndoLog(
            {UndoLog{false, std::move(modified_fields), undo_tuple, base_meta.ts_, *pre_undo_link}});
        txn_mgr_->UpdateUndoLink(child_iter->first, undo_link);
      } else {
        // Compute expressions
        auto pre_undo_link = txn_mgr_->GetUndoLink(child_iter->first);
        if (pre_undo_link.has_value() && pre_undo_link->IsValid()) {
          auto pre_undo_log = txn_mgr_->GetUndoLog(*pre_undo_link);
          std::vector<Value> old_values;
          std::vector<bool> modified_fields(table_info_->schema_.GetColumnCount(), false);
          const Schema *schema_ptr = &table_info_->schema_;
          auto prev_undo_schema = GetUndoLogSchema(schema_ptr, pre_undo_log);
          uint32_t undo_val_idx = 0;
          for (size_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
            if (pre_undo_log.modified_fields_.at(i)) {
              Value old_value = pre_undo_log.tuple_.GetValue(&prev_undo_schema, undo_val_idx);
              old_values.emplace_back(std::move(old_value));
              undo_val_idx++;
              modified_fields.at(i) = true;
            } else {
              Value old_value = child_iter->second.GetValue(&table_info_->schema_, i);
              Value new_value = new_tuple.GetValue(&table_info_->schema_, i);
              if (!old_value.CompareExactlyEquals(new_value)) {
                modified_fields.at(i) = true;
                old_values.emplace_back(std::move(old_value));
              }
            }
          }
          auto undo_schema = ConstructUndoLogSchema(schema_ptr, modified_fields);
          Tuple undo_tuple{old_values, &undo_schema};
          UndoLog undolog{pre_undo_log.is_deleted_, std::move(modified_fields), undo_tuple, pre_undo_log.ts_,
                          pre_undo_log.prev_version_};
          txn_->ModifyUndoLog(pre_undo_link->prev_log_idx_, undolog);
        }
      }
    }

    // heap更新
    TupleMeta update_tuple_meta{txn_->GetTransactionTempTs(), false};
    table_info_->table_->UpdateTupleInPlace(update_tuple_meta, new_tuple, child_iter->first);
    temp_updated_rids_.emplace_back(child_iter->first);
    child_iter++;
  }
  Value ret_value = ValueFactory::GetIntegerValue(temp_updated_rids_.size());
  std::vector<Value> ret_value_vec;
  ret_value_vec.reserve(GetOutputSchema().GetColumnCount());
  ret_value_vec.emplace_back(ret_value);
  *tuple = Tuple{ret_value_vec, &GetOutputSchema()};

  for (const auto update_rid : temp_updated_rids_) {
    txn_->AppendWriteSet(table_info_->oid_, update_rid);
  }
  return true;
}
// p3
// auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   if (if_executed_) {
//     return false;
//   }
//   if_executed_ = true;
//   Catalog *catalog = exec_ctx_->GetCatalog();
//   auto table_indexs = catalog->GetTableIndexes(table_info_->name_);
//   Tuple update_tuple;
//   RID update_old_rid;
//   std::vector<std::pair<RID, RID>> temp_update_old_new_rids;
//   while (child_executor_->Next(&update_tuple, &update_old_rid)) {
//     // Compute expressions
//     std::vector<Value> values{};
//     values.reserve(table_info_->schema_.GetColumnCount());
//     for (size_t i = 0; i < plan_->target_expressions_.size(); i++) {
//       const auto &expr = plan_->target_expressions_[i];
//       Value new_value = expr->Evaluate(&update_tuple, table_info_->schema_);
//       if (new_value.IsNull() || new_value.GetTypeId() != table_info_->schema_.GetColumn(i).GetType()) {
//         UndoUpdate(temp_update_old_new_rids);
//         throw bustub::Exception("update table schema mismatch");
//       }
//       values.push_back(expr->Evaluate(&update_tuple, table_info_->schema_));
//     }
//     Tuple new_tupe(values, &table_info_->schema_);

//     // 插新record
//     TupleMeta insert_tuple_meta{INVALID_TS, false};
//     auto update_new_rid = table_info_->table_->InsertTuple(insert_tuple_meta, new_tupe, exec_ctx_->GetLockManager(),
//                                                            exec_ctx_->GetTransaction(), table_info_->oid_);
//     if (!update_new_rid.has_value()) {
//       UndoUpdate(temp_update_old_new_rids);
//       throw bustub::Exception("insert failed has no empty space");
//     }
//     temp_update_old_new_rids.emplace_back(update_old_rid, update_new_rid.value());

//     // 删旧record
//     TupleMeta update_tuple_meta{INVALID_TS, true};
//     table_info_->table_->UpdateTupleMeta(update_tuple_meta, update_old_rid);

//     for (auto &index : table_indexs) {
//       // 删旧索引
//       Tuple old_index_tuple =
//           update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
//       index->index_->DeleteEntry(old_index_tuple, update_old_rid, exec_ctx_->GetTransaction());

//       // 插新索引
//       Tuple new_index_tuple =
//           new_tupe.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
//       if (!index->index_->InsertEntry(new_index_tuple, update_new_rid.value(), exec_ctx_->GetTransaction())) {
//         UndoUpdate(temp_update_old_new_rids);
//         throw bustub::Exception("insert failed duplicate index");
//       }
//     }
//   }
//   Value ret_value = ValueFactory::GetIntegerValue(temp_update_old_new_rids.size());
//   std::vector<Value> ret_value_vec;
//   ret_value_vec.reserve(GetOutputSchema().GetColumnCount());
//   ret_value_vec.emplace_back(ret_value);
//   *tuple = Tuple{ret_value_vec, &GetOutputSchema()};
//   return true;
// }

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
