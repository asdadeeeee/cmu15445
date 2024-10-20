#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

auto GetUndoLogSchema(const Schema *&schema, const UndoLog &undo_log) -> Schema;
auto ConstructUndoLogSchema(const Schema *&schema, const std::vector<bool> &modified_fields) -> Schema;
auto CanTupleBeSeen(timestamp_t tuple_ts, Transaction *txn) -> bool;

auto CollectUndoLogs(const TupleMeta &base_meta, TransactionManager *txn_mgr, Transaction *curr_trx, RID rid)
    -> std::vector<UndoLog>;

auto IsWriteWriteNotConflict(std::optional<VersionUndoLink> version_link) -> bool;

auto ConstructUndoLogFromBase(TableInfo *table_info, TransactionManager *txn_mgr, RID rid) -> UndoLog;
// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs /done

// todo(zhouzj) 应该要有这个，声明里加回调函数，所有需要遍历undo的功能都调这个
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema /done
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
