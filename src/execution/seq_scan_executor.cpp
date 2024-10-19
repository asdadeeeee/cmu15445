//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t insert_table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(insert_table_oid);
  table_iter_ = std::make_shared<TableIterator>(table_info->table_->MakeIterator());
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    auto tuple_pair = table_iter_->GetTuple();
    // if(tuple_pair.first.ts_)
    std::vector<UndoLog> undologs = CollectUndoLogs(tuple_pair.first, txn_mgr_, txn_, table_iter_->GetRID());
    auto temp_tuple = ReconstructTuple(&GetOutputSchema(), tuple_pair.second, tuple_pair.first, undologs);
    if (!temp_tuple.has_value()) {
      ++(*table_iter_);
      continue;
    }
    // if (tuple_pair.first.is_deleted_) {
    //   ++(*table_iter_);
    //   continue;
    // }
    // 添加predicate
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&temp_tuple.value(), GetOutputSchema());
      if (!value.IsNull() && !value.GetAs<bool>()) {
        ++(*table_iter_);
        continue;
      }
    }
    *tuple = Tuple(std::move(temp_tuple.value()));
    *rid = RID(table_iter_->GetRID().GetPageId(), table_iter_->GetRID().GetSlotNum());
    ++(*table_iter_);
    return true;
  }
  return false;
}

// p3
// auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   while (!table_iter_->IsEnd()) {
//     auto tuple_pair = table_iter_->GetTuple();
//     if (tuple_pair.first.is_deleted_) {
//       ++(*table_iter_);
//       continue;
//     }
//     // 添加predicate
//     if (plan_->filter_predicate_ != nullptr) {
//       auto value = plan_->filter_predicate_->Evaluate(&tuple_pair.second, GetOutputSchema());
//       if (!value.IsNull() && !value.GetAs<bool>()) {
//         ++(*table_iter_);
//         continue;
//       }
//     }
//     *tuple = Tuple(tuple_pair.second);
//     *rid = RID(table_iter_->GetRID().GetPageId(), table_iter_->GetRID().GetSlotNum());
//     ++(*table_iter_);
//     return true;
//   }
//   return false;
// }
}  // namespace bustub
