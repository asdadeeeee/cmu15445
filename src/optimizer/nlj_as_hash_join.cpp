#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    bool can_be_as_hash_join = true;
    GetHashKeyExpr(nlj_plan.Predicate(), left_key_expressions, right_key_expressions,
                   std::make_shared<const NestedLoopJoinPlanNode>(nlj_plan), can_be_as_hash_join);
    if (can_be_as_hash_join) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                std::move(right_key_expressions), nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}
void Optimizer::GetHashKeyExpr(const bustub::AbstractExpressionRef &filter_predicate,
                               std::vector<AbstractExpressionRef> &left_key_expressions,
                               std::vector<AbstractExpressionRef> &right_key_expressions,
                               const bustub::AbstractPlanNodeRef &nlj_plan_node, bool &can_be_as_hash_join) {
  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan_node);
  if (auto compare_expr = std::dynamic_pointer_cast<ComparisonExpression>(filter_predicate); compare_expr != nullptr) {
    if (compare_expr->comp_type_ == ComparisonType::Equal) {
      const auto left_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->children_[0]);
      const auto right_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->children_[1]);
      if (left_expr != nullptr && right_expr != nullptr) {
        // Ensure right child is table scan
        if (nlj_plan.GetRightPlan()->GetType() == PlanType::SeqScan) {
          if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
            left_key_expressions.emplace_back(left_expr);
            right_key_expressions.emplace_back(right_expr);
          }
          if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
            left_key_expressions.emplace_back(right_expr);
            right_key_expressions.emplace_back(left_expr);
          }
        } else {
          if (can_be_as_hash_join) {
            can_be_as_hash_join = false;
          }
        }
      } else {
        if (can_be_as_hash_join) {
          can_be_as_hash_join = false;
        }
      }
    }
  } else if (auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(filter_predicate); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      GetHashKeyExpr(logic_expr->GetChildAt(0), left_key_expressions, right_key_expressions, nlj_plan_node,
                     can_be_as_hash_join);
      GetHashKeyExpr(logic_expr->GetChildAt(1), left_key_expressions, right_key_expressions, nlj_plan_node,
                     can_be_as_hash_join);
    }
  } else {
    if (can_be_as_hash_join) {
      can_be_as_hash_join = false;
    }
  }
}

}  // namespace bustub
