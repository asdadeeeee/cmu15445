//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  if_executed_ = false;
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (if_executed_) {
    return false;
  }
  if_executed_ = true;
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t insert_table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(insert_table_oid);
  auto table_indexs = catalog->GetTableIndexes(table_info->name_);
  Tuple insert_tuple;
  RID temp_rid;
  std::vector<RID> temp_inserted_rids;
  while (child_executor_->Next(&insert_tuple, &temp_rid)) {
    for (auto &index : table_indexs) {
      Tuple index_tuple =
          insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      std::vector<RID> exist_rids;
      exist_rids.clear();
      index->index_->ScanKey(index_tuple, &exist_rids, txn_);
      if (!exist_rids.empty()) {
        // UndoInsert(temp_inserted_rids);
        txn_->SetTainted();
        throw bustub::ExecutionException("insert failed exist duplicate index before make heap tuple");
        return false;
      }
    }

    // TupleMeta insert_tuple_meta{INVALID_TS, false};
    TupleMeta insert_tuple_meta{txn_->GetTransactionTempTs(), false};
    auto inserted_rid = table_info->table_->InsertTuple(insert_tuple_meta, insert_tuple, exec_ctx_->GetLockManager(),
                                                        exec_ctx_->GetTransaction(), insert_table_oid);
    if (!inserted_rid.has_value()) {
      UndoInsert(temp_inserted_rids);
      throw bustub::Exception("insert failed has no empty space");
      return false;
    }
    temp_inserted_rids.emplace_back(inserted_rid.value());
    for (auto &index : table_indexs) {
      Tuple index_tuple =
          insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      if (!index->index_->InsertEntry(index_tuple, inserted_rid.value(), exec_ctx_->GetTransaction())) {
        // UndoInsert(temp_inserted_rids);
        txn_->SetTainted();
        throw bustub::ExecutionException("insert failed exist duplicate index when insert index after make heap tuple");
        return false;
      }
    }
  }

  Value ret_value = ValueFactory::GetIntegerValue(temp_inserted_rids.size());
  std::vector<Value> ret_value_vec;
  ret_value_vec.reserve(GetOutputSchema().GetColumnCount());
  ret_value_vec.emplace_back(ret_value);
  *tuple = Tuple{ret_value_vec, &GetOutputSchema()};

  for (const auto insert_rid : temp_inserted_rids) {
    txn_->AppendWriteSet(table_info->oid_, insert_rid);
  }
  return true;
}

// 删历史record
// 删索引
void InsertExecutor::UndoInsert(std::vector<RID> &temp_inserted_rids) {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t insert_table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(insert_table_oid);
  auto table_indexs = catalog->GetTableIndexes(table_info->name_);
  for (auto &inserted_rid : temp_inserted_rids) {
    auto inserted_tuple_pair = table_info->table_->GetTuple(inserted_rid);
    for (auto &index : table_indexs) {
      Tuple inserted_tuple = inserted_tuple_pair.second;
      Tuple index_tuple =
          inserted_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(index_tuple, inserted_rid, exec_ctx_->GetTransaction());
    }
    TupleMeta inserted_tuple_meta = inserted_tuple_pair.first;
    inserted_tuple_meta.ts_ = 0;  // ts为0，就好像这条tuple从来没有出现过
    inserted_tuple_meta.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(inserted_tuple_meta, inserted_rid);
  }
}

// P3
// auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   if (if_executed_) {
//     return false;
//   }
//   if_executed_ = true;
//   Catalog *catalog = exec_ctx_->GetCatalog();
//   table_oid_t insert_table_oid = plan_->GetTableOid();
//   TableInfo *table_info = catalog->GetTable(insert_table_oid);
//   auto table_indexs = catalog->GetTableIndexes(table_info->name_);
//   Tuple insert_tuple;
//   RID temp_rid;
//   std::vector<RID> temp_inserted_rids;
//   while (child_executor_->Next(&insert_tuple, &temp_rid)) {
//     TupleMeta insert_tuple_meta{INVALID_TS, false};
//     auto inserted_rid = table_info->table_->InsertTuple(insert_tuple_meta, insert_tuple, exec_ctx_->GetLockManager(),
//                                                         exec_ctx_->GetTransaction(), insert_table_oid);
//     if (!inserted_rid.has_value()) {
//       UndoInsert(temp_inserted_rids);
//       throw bustub::Exception("insert failed has no empty space");
//       return false;
//     }
//     temp_inserted_rids.emplace_back(inserted_rid.value());
//     for (auto &index : table_indexs) {
//       Tuple index_tuple =
//           insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
//       if (!index->index_->InsertEntry(index_tuple, inserted_rid.value(), exec_ctx_->GetTransaction())) {
//         UndoInsert(temp_inserted_rids);
//         throw bustub::Exception("insert failed duplicate index");
//         return false;
//       }
//     }
//   }
//   Value ret_value = ValueFactory::GetIntegerValue(temp_inserted_rids.size());
//   std::vector<Value> ret_value_vec;
//   ret_value_vec.reserve(GetOutputSchema().GetColumnCount());
//   ret_value_vec.emplace_back(ret_value);
//   *tuple = Tuple{ret_value_vec, &GetOutputSchema()};
//   return true;
// }
}  // namespace bustub
