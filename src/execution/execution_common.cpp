#include "execution/execution_common.h"
#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  auto undo_iter = undo_logs.begin();
  std::optional<Tuple> result;
  if (base_meta.is_deleted_) {
    result = std::nullopt;
  } else {
    result = Tuple(base_tuple);
  }

  while (undo_iter != undo_logs.end()) {
    // base meta内delete若为false则没有问题 若也为true？
    if (undo_iter->is_deleted_) {
      result = std::nullopt;
    } else {
      std::vector<Value> merge_values;

      auto undo_schema = GetUndoLogSchema(schema, *undo_iter);
      uint32_t undo_val_idx = 0;
      for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
        Value v;
        if (undo_iter->modified_fields_[idx]) {
          v = undo_iter->tuple_.GetValue(&undo_schema, undo_val_idx);
          undo_val_idx++;
        } else {
          v = result.value().GetValue(schema, idx);
        }
        merge_values.emplace_back(std::move(v));
      }
      result = Tuple(merge_values, schema);
    }
    undo_iter++;
  }
  return result;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  auto table_iter = table_heap->MakeIterator();
  while (!table_iter.IsEnd()) {
    std::string ts;
    if (table_iter.GetTuple().first.ts_ >= TXN_START_ID) {
      ts = "txn" + std::to_string(table_iter.GetTuple().first.ts_ ^ TXN_START_ID);
    } else {
      ts = std::to_string(table_iter.GetTuple().first.ts_);
    }
    fmt::println(stderr, "RID={}/{}, ts={}, delete={}, tuple={}", table_iter.GetRID().GetPageId(),
                 table_iter.GetRID().GetSlotNum(), ts, table_iter.GetTuple().first.is_deleted_,
                 table_iter.GetTuple().second.ToString(&table_info->schema_));
    auto undo_link = txn_mgr->GetUndoLink(table_iter.GetRID());
    const Schema *schema_ptr = &(table_info->schema_);
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto txn_iter = txn_mgr->txn_map_.find(undo_link->prev_txn_);
      if (txn_iter == txn_mgr->txn_map_.end()) {
        fmt::println(stderr, "txn not exist");
        break;
      }
      auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
      auto undo_schema = GetUndoLogSchema(schema_ptr, undo_log);
      std::string tuple_str;
      if (!undo_log.is_deleted_) {
        tuple_str += "(";
        uint32_t undo_val_idx = 0;
        for (const auto &if_modify : undo_log.modified_fields_) {
          if (if_modify) {
            tuple_str += undo_log.tuple_.GetValue(&undo_schema, undo_val_idx).ToString() + ",";
            undo_val_idx++;
          } else {
            tuple_str += " _,";
          }
        }
        tuple_str += " )";
        fmt::println(stderr, "    txn{}@{}, {} ts={}", txn_iter->second->GetTransactionIdHumanReadable(),
                     txn_iter->second->GetTransactionState(), tuple_str, undo_log.ts_);
      } else {
        fmt::println(stderr, "    txn{}@{}, <del> ts={}", txn_iter->second->GetTransactionIdHumanReadable(),
                     txn_iter->second->GetTransactionState(), undo_log.ts_);
      }
      undo_link = undo_log.prev_version_;
    }
    ++table_iter;
  }

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto ConstructUndoLogSchema(const Schema *&schema, const std::vector<bool> &modified_fields) -> Schema {
  std::vector<uint32_t> undo_column_idxs;
  for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
    if (modified_fields[idx]) {
      undo_column_idxs.emplace_back(idx);
    }
  }
  return Schema::CopySchema(schema, undo_column_idxs);
}

auto GetUndoLogSchema(const Schema *&schema, const UndoLog &undo_log) -> Schema {
  std::vector<uint32_t> undo_column_idxs;
  for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
    if (undo_log.modified_fields_[idx]) {
      undo_column_idxs.emplace_back(idx);
    }
  }
  return Schema::CopySchema(schema, undo_column_idxs);
}

auto CanTupleBeSeen(timestamp_t tuple_ts, Transaction *txn) -> bool {
  return tuple_ts == txn->GetTransactionTempTs() || tuple_ts <= txn->GetReadTs();
}

auto CollectUndoLogs(const TupleMeta &base_meta, TransactionManager *txn_mgr, Transaction *curr_trx, RID rid)
    -> std::vector<UndoLog> {
  std::vector<UndoLog> undo_logs;
  if (CanTupleBeSeen(base_meta.ts_, curr_trx)) {
    return undo_logs;
  }
  auto undo_link = txn_mgr->GetUndoLink(rid);
  while (undo_link.has_value() && undo_link->IsValid()) {
    auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
    undo_logs.emplace_back(undo_log);
    if (CanTupleBeSeen(undo_log.ts_, curr_trx)) {
      return undo_logs;
    }
    undo_link = undo_log.prev_version_;
  }
  // 到此说明所有的undo 都看不到
  auto delete_undo_log = UndoLog{true, {}, Tuple{}};
  undo_logs.emplace_back(std::move(delete_undo_log));
  return undo_logs;
}

auto IsWriteWriteNotConflict(std::optional<VersionUndoLink> version_link) -> bool {
  return !(version_link.has_value() && version_link->in_progress_);
}

auto ConstructUndoLogFromBase(TableInfo *table_info, TransactionManager *txn_mgr, RID rid) -> UndoLog {
  auto pair = table_info->table_->GetTuple(rid);
  std::vector<bool> modified_fields(table_info->schema_.GetColumnCount(), true);
  auto pre_undo_link = txn_mgr->GetUndoLink(rid);
  if (!(pre_undo_link.has_value() && pre_undo_link->IsValid())) {
    pre_undo_link = UndoLink{};
  }
  return UndoLog{pair.first.is_deleted_, std::move(modified_fields), pair.second, pair.first.ts_, *pre_undo_link};
}
}  // namespace bustub
