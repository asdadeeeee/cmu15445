#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "concurrency/transaction.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    BUSTUB_ASSERT(optimized_plan->children_.empty(), "must have exactly no children");
    const auto &seqscan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    ConstantValueExpression *pred_keys;
    std::vector<const ColumnValueExpression *> indexed_columns;
    GetKeysFromFilter(seqscan_plan.filter_predicate_, pred_keys, indexed_columns);
    index_oid_t index_id = 0;
    if (CanSeqScanBeIndexScan(std::make_shared<const SeqScanPlanNode>(seqscan_plan), index_id, indexed_columns)) {
      return std::make_shared<IndexScanPlanNode>(seqscan_plan.output_schema_, seqscan_plan.table_oid_, index_id,
                                                 seqscan_plan.filter_predicate_, pred_keys);
    }
  }

  return optimized_plan;
}

void Optimizer::GetKeysFromFilter(const bustub::AbstractExpressionRef &filter_predicate,
                                  ConstantValueExpression * &pred_keys,
                                  std::vector<const ColumnValueExpression *> &indexed_columns) {
  auto comare_expr = std::dynamic_pointer_cast<ComparisonExpression>(filter_predicate);
  if (comare_expr) {
    if (comare_expr->comp_type_ == ComparisonType::Equal) {
      auto left_column = std::dynamic_pointer_cast<ColumnValueExpression>(comare_expr->GetChildAt(0));
      auto right_column = std::dynamic_pointer_cast<ColumnValueExpression>(comare_expr->GetChildAt(1));
      if (left_column) {
        auto right_value = std::dynamic_pointer_cast<ConstantValueExpression>(comare_expr->GetChildAt(1));
        if (right_value) {
          indexed_columns.emplace_back(left_column.get());
          // 本来应该这样的 fall2023的问题
          // pred_keys.emplace_back(right_value.get());
          pred_keys = right_value.get();
        }
      }
      if (right_column) {
        auto left_value = std::dynamic_pointer_cast<ConstantValueExpression>(comare_expr->GetChildAt(0));
        if (left_value) {
          indexed_columns.emplace_back(right_column.get());
          // pred_keys.emplace_back(left_value.get());
          pred_keys = left_value.get();
        }
      }
    }
  }
  auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(filter_predicate);
  if (logic_expr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      GetKeysFromFilter(logic_expr->GetChildAt(0), pred_keys, indexed_columns);
      GetKeysFromFilter(logic_expr->GetChildAt(1), pred_keys, indexed_columns);
    }
  }
}

auto Optimizer::CanSeqScanBeIndexScan(const bustub::AbstractPlanNodeRef &seq_plan, index_oid_t &index_id,
                                      const std::vector<const ColumnValueExpression *> &indexed_columns) -> bool {
  const auto &seqscan_plan = dynamic_cast<const SeqScanPlanNode &>(*seq_plan);
  auto indexes = catalog_.GetTableIndexes(seqscan_plan.table_name_);
  for (const auto &index : indexes) {
    if (index->index_->GetIndexColumnCount() == indexed_columns.size()) {
      std::vector<uint32_t> temp_attrs;
      for (uint32_t attr : index->index_->GetKeyAttrs()) {
        temp_attrs.emplace_back(attr);
      }
      for (const auto &filter_column : indexed_columns) {
        // 避免索引(v1,v2) filter v1=1 and v1=2
        auto iter = std::find(temp_attrs.begin(), temp_attrs.end(), filter_column->GetColIdx());
        if (iter != temp_attrs.end()) {
          temp_attrs.erase(iter);
        }
      }
      if (temp_attrs.empty()) {
        index_id = index->index_oid_;
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
