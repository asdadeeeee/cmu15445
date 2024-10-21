//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  if_executed_ = false;
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (if_executed_) {
    return false;
  }
  if_executed_ = true;
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t delete_table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(delete_table_oid);
  auto table_indexs = catalog->GetTableIndexes(table_info->name_);
  Tuple delete_tuple;
  RID delete_rid;
  int32_t num = 0;
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    auto base_meta = table_info->table_->GetTupleMeta(delete_rid);
    // heap meta写写冲突
    if (!CanTupleBeSeen(base_meta.ts_, txn_)) {
      txn_->SetTainted();
      throw ExecutionException("Delete write conflict base_meta");
    }

    // 加入写集
    txn_->AppendWriteSet(table_info->oid_, delete_rid);

    // 如果ts不等 说明该txn_没有修改过这个tuple，那么需要构造undo,并update
    if (base_meta.ts_ != txn_->GetTransactionTempTs()) {
      auto undo_log = ConstructUndoLogFromBase(table_info, txn_mgr_, delete_rid);
      auto undo_link = txn_->AppendUndoLog(undo_log);
      txn_mgr_->UpdateUndoLink(delete_rid, undo_link);
    } else {
      auto pre_undo_link = txn_mgr_->GetUndoLink(delete_rid);
      if (pre_undo_link.has_value() && pre_undo_link->IsValid()) {
        auto pre_undo_log = txn_mgr_->GetUndoLog(*pre_undo_link);
        std::vector<Value> old_values;
        std::vector<bool> modified_fields(table_info->schema_.GetColumnCount(), false);
        const Schema *schema_ptr = &table_info->schema_;
        auto prev_undo_schema = GetUndoLogSchema(schema_ptr, pre_undo_log);
        uint32_t undo_val_idx = 0;
        for (size_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
          Value old_value;
          if (pre_undo_log.modified_fields_.at(i)) {
            old_value = pre_undo_log.tuple_.GetValue(&prev_undo_schema, undo_val_idx);
            undo_val_idx++;

          } else {
            old_value = delete_tuple.GetValue(&table_info->schema_, i);
          }
          modified_fields.at(i) = true;
          old_values.emplace_back(std::move(old_value));
        }
        auto undo_schema = ConstructUndoLogSchema(schema_ptr, modified_fields);
        Tuple undo_tuple{old_values, &undo_schema};
        UndoLog undolog{false, std::move(modified_fields), undo_tuple, pre_undo_log.ts_, pre_undo_log.prev_version_};
        txn_->ModifyUndoLog(pre_undo_link->prev_log_idx_, undolog);
      }
    }

    // MVCC中保留索引
    // 索引删除
    // for (auto &index : table_indexs) {
    //   Tuple index_tuple =
    //       delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    //   index->index_->DeleteEntry(index_tuple, delete_rid, exec_ctx_->GetTransaction());
    // }

    // heap删除
    TupleMeta delete_tuple_meta{txn_->GetTransactionTempTs(), true};
    table_info->table_->UpdateTupleMeta(delete_tuple_meta, delete_rid);
    num++;
  }
  Value ret_value = ValueFactory::GetIntegerValue(num);
  std::vector<Value> ret_value_vec;
  ret_value_vec.reserve(GetOutputSchema().GetColumnCount());
  ret_value_vec.emplace_back(ret_value);
  *tuple = Tuple{ret_value_vec, &GetOutputSchema()};
  return true;
}
// p3
// auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   if (if_executed_) {
//     return false;
//   }
//   if_executed_ = true;
//   Catalog *catalog = exec_ctx_->GetCatalog();
//   table_oid_t insert_table_oid = plan_->GetTableOid();
//   TableInfo *table_info = catalog->GetTable(insert_table_oid);
//   auto table_indexs = catalog->GetTableIndexes(table_info->name_);
//   Tuple delete_tuple;
//   RID delete_rid;
//   int32_t num = 0;
//   while (child_executor_->Next(&delete_tuple, &delete_rid)) {
//     for (auto &index : table_indexs) {
//       Tuple index_tuple =
//           delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
//       index->index_->DeleteEntry(index_tuple, delete_rid, exec_ctx_->GetTransaction());
//     }
//     TupleMeta delete_tuple_meta{INVALID_TS, true};
//     table_info->table_->UpdateTupleMeta(delete_tuple_meta, delete_rid);
//     num++;
//   }
//   Value ret_value = ValueFactory::GetIntegerValue(num);
//   std::vector<Value> ret_value_vec;
//   ret_value_vec.reserve(GetOutputSchema().GetColumnCount());
//   ret_value_vec.emplace_back(ret_value);
//   *tuple = Tuple{ret_value_vec, &GetOutputSchema()};
//   return true;
// }

}  // namespace bustub
