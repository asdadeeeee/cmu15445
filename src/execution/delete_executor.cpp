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

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  if_executed_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (if_executed_) {
    return false;
  }
  if_executed_ = true;
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t insert_table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(insert_table_oid);
  auto table_indexs = catalog->GetTableIndexes(table_info->name_);
  Tuple delete_tuple;
  RID delete_rid;
  int32_t num = 0;
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    for (auto &index : table_indexs) {
      Tuple index_tuple =
          delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(index_tuple, delete_rid, exec_ctx_->GetTransaction());
    }
    TupleMeta delete_tuple_meta{INVALID_TS, true};
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

}  // namespace bustub
