//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_.load());
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  for (const auto &write_table : txn->write_set_) {
    TableInfo *table_info = catalog_->GetTable(write_table.first);
    for (const auto &write_rid : write_table.second) {
      bool if_delete = table_info->table_->GetTupleMeta(write_rid).is_deleted_;
      TupleMeta commit_tuple_meta{commit_ts, if_delete};
      table_info->table_->UpdateTupleMeta(commit_tuple_meta, write_rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_.store(commit_ts);
  last_commit_ts_.fetch_add(1);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  auto all_table_names = catalog_->GetTableNames();
  std::set<txn_id_t> useful_txn_ids;
  for (const auto &table_name : all_table_names) {
    auto table_info = catalog_->GetTable(table_name);
    auto table_iter = table_info->table_->MakeIterator();
    while (!table_iter.IsEnd()) {
      auto base_meta = table_info->table_->GetTupleMeta(table_iter.GetRID());
      if (base_meta.ts_ > GetWatermark()) {
        auto undo_link = GetUndoLink(table_iter.GetRID());
        while (undo_link.has_value() && undo_link->IsValid()) {
          auto undo_log = GetUndoLogOptional(undo_link.value());
          if (undo_log.has_value()) {
            useful_txn_ids.insert(undo_link->prev_txn_);
            if (undo_log->ts_ <= GetWatermark()) {
              break;
            }
            undo_link = undo_log->prev_version_;
          } else {
            break;
          }
        }
      }

      ++table_iter;
    }
  }
  auto iter = txn_map_.begin();
  while (iter != txn_map_.end()) {
    if (useful_txn_ids.find(iter->first) == useful_txn_ids.end() &&
        (iter->second->GetTransactionState() == TransactionState::ABORTED ||
         iter->second->GetTransactionState() == TransactionState::COMMITTED)) {
      txn_map_.erase(iter++);
    } else {
      iter++;
    }
  }
}

}  // namespace bustub
