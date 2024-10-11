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
#include "execution/plans/mock_scan_plan.h"
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

  // 在递归生成HashJoinPlan前 所有nlj filter需要被递归改写 即下推

  RewriteNLJpredicacte(const_cast<AbstractPlanNode &>(*plan));

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
  // const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan_node);
  if (auto compare_expr = std::dynamic_pointer_cast<ComparisonExpression>(filter_predicate); compare_expr != nullptr) {
    if (compare_expr->comp_type_ == ComparisonType::Equal) {
      const auto left_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->children_[0]);
      const auto right_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->children_[1]);
      if (left_expr != nullptr && right_expr != nullptr) {
        if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
          left_key_expressions.emplace_back(left_expr);
          right_key_expressions.emplace_back(right_expr);
        } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
          left_key_expressions.emplace_back(right_expr);
          right_key_expressions.emplace_back(left_expr);
        } else {
          can_be_as_hash_join = false;
        }
      } else {
        can_be_as_hash_join = false;
      }
    } else {
      can_be_as_hash_join = false;
    }
  } else if (auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(filter_predicate); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      GetHashKeyExpr(logic_expr->GetChildAt(0), left_key_expressions, right_key_expressions, nlj_plan_node,
                     can_be_as_hash_join);
      GetHashKeyExpr(logic_expr->GetChildAt(1), left_key_expressions, right_key_expressions, nlj_plan_node,
                     can_be_as_hash_join);
    } else {
      can_be_as_hash_join = false;
    }
  } else {
    can_be_as_hash_join = false;
  }
}

void Optimizer::GetPredicateExpr(const bustub::AbstractExpressionRef &filter_predicate,
                                 std::vector<AbstractExpressionRef> &root_predicate_expressions,
                                 std::vector<AbstractExpressionRef> &left_predicate_expressions,
                                 std::vector<AbstractExpressionRef> &right_predicate_expressions, bool &have_or) {
  if (auto compare_expr = std::dynamic_pointer_cast<ComparisonExpression>(filter_predicate); compare_expr != nullptr) {
    const auto left_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->children_[0]);
    const auto right_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->children_[1]);
    if (left_expr != nullptr && right_expr != nullptr) {
      // 此处不用管两列比较是否为"=" 非"="的情况会在GetHashKeyExpr被拦住 判断为无法转为hashjoin
      if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 0) {
        left_predicate_expressions.emplace_back(compare_expr);
      } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 1) {
        right_predicate_expressions.emplace_back(compare_expr);
      } else {
        root_predicate_expressions.emplace_back(compare_expr);
      }
    } else if (left_expr != nullptr && right_expr == nullptr) {
      if (left_expr->GetTupleIdx() == 0) {
        left_predicate_expressions.emplace_back(compare_expr);
      } else if (left_expr->GetTupleIdx() == 1) {
        right_predicate_expressions.emplace_back(compare_expr);
      }
    } else if (left_expr == nullptr && right_expr != nullptr) {
      if (right_expr->GetTupleIdx() == 0) {
        left_predicate_expressions.emplace_back(compare_expr);
      } else if (right_expr->GetTupleIdx() == 1) {
        right_predicate_expressions.emplace_back(compare_expr);
      }
    } else {
      root_predicate_expressions.emplace_back(compare_expr);
    }

  } else if (auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(filter_predicate); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      GetPredicateExpr(logic_expr->GetChildAt(0), root_predicate_expressions, left_predicate_expressions,
                       right_predicate_expressions, have_or);
      GetPredicateExpr(logic_expr->GetChildAt(1), root_predicate_expressions, left_predicate_expressions,
                       right_predicate_expressions, have_or);
    } else {
      have_or = true;
    }
  }
}

auto Optimizer::GatherPredicate(bustub::AbstractExpressionRef &predicate,
                                std::vector<AbstractExpressionRef> &predicate_expressions) -> bool {
  if (predicate_expressions.empty()) {
    predicate = bustub::AbstractExpressionRef(nullptr);
  } else {
    auto iter = predicate_expressions.begin();
    predicate = *iter;
    iter++;
    while (iter != predicate_expressions.end()) {
      bool can_combine = true;
      predicate = CombinePredicate(predicate, *iter, can_combine);
      if (!can_combine) {
        return false;
      }
      iter++;
    }
  }
  return true;
}

void Optimizer::RewriteNLJpredicacte(AbstractPlanNode &nlj_plan_node) {
  if (nlj_plan_node.GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(nlj_plan_node);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    std::vector<AbstractExpressionRef> root_predicate_expressions;
    std::vector<AbstractExpressionRef> left_predicate_expressions;
    std::vector<AbstractExpressionRef> right_predicate_expressions;
    bool have_or = false;
    GetPredicateExpr(nlj_plan.Predicate(), root_predicate_expressions, left_predicate_expressions,
                     right_predicate_expressions, have_or);

    if (have_or) {
      return;
    }
    auto root_predicate = bustub::AbstractExpressionRef(nullptr);
    auto left_predicate = bustub::AbstractExpressionRef(nullptr);
    auto right_predicate = bustub::AbstractExpressionRef(nullptr);

    if (!GatherPredicate(root_predicate, root_predicate_expressions)) {
      return;
    }
    if (!GatherPredicate(left_predicate, left_predicate_expressions)) {
      return;
    }
    if (!GatherPredicate(right_predicate, right_predicate_expressions)) {
      return;
    }
    if (root_predicate != nullptr) {
      nlj_plan.predicate_ = root_predicate;
    }
    if (auto const_left_plan = std::dynamic_pointer_cast<const NestedLoopJoinPlanNode>(nlj_plan.GetChildAt(0));
        const_left_plan != nullptr) {
      auto &left_plan = dynamic_cast<NestedLoopJoinPlanNode &>(const_cast<AbstractPlanNode &>(*nlj_plan.children_[0]));
      if (left_predicate != nullptr) {
        left_plan.predicate_ =
            RewriteExpressionForJoin(left_predicate, left_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
                                     left_plan.GetRightPlan()->OutputSchema().GetColumnCount());
      }
      RewriteNLJpredicacte(left_plan);
    } else {
      if (left_predicate != nullptr) {
        auto schema = nlj_plan.GetChildAt(0)->OutputSchema();
        nlj_plan.children_[0] = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(schema),
                                                                 std::move(left_predicate), nlj_plan.GetChildAt(0));
      }
    }

    if (auto const_right_plan = std::dynamic_pointer_cast<const NestedLoopJoinPlanNode>(nlj_plan.GetChildAt(1));
        const_right_plan != nullptr) {
      auto &right_plan = dynamic_cast<NestedLoopJoinPlanNode &>(const_cast<AbstractPlanNode &>(*nlj_plan.children_[1]));
      if (right_predicate != nullptr) {
        right_plan.predicate_ =
            RewriteExpressionForJoin(right_predicate, right_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
                                     right_plan.GetRightPlan()->OutputSchema().GetColumnCount());
      }
      RewriteNLJpredicacte(right_plan);
    } else {
      if (right_predicate != nullptr) {
        auto schema = nlj_plan.GetChildAt(1)->OutputSchema();
        nlj_plan.children_[1] = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(schema),
                                                                 std::move(right_predicate), nlj_plan.GetChildAt(1));
      }
    }
  }
}
auto Optimizer::CombinePredicate(const bustub::AbstractExpressionRef &left_predicate,
                                 const bustub::AbstractExpressionRef &right_predicate, bool &can_be_combine)
    -> AbstractExpressionRef {
  if (auto left_expr = std::dynamic_pointer_cast<LogicExpression>(left_predicate); left_expr != nullptr) {
    if (left_expr->logic_type_ != LogicType::And) {
      can_be_combine = false;
    }
  } else if (auto left_expr = std::dynamic_pointer_cast<ComparisonExpression>(left_predicate); left_expr == nullptr) {
    can_be_combine = false;
  }

  if (auto right_expr = std::dynamic_pointer_cast<LogicExpression>(right_predicate); right_expr != nullptr) {
    if (right_expr->logic_type_ != LogicType::And) {
      can_be_combine = false;
    }
  } else if (auto right_expr = std::dynamic_pointer_cast<ComparisonExpression>(right_predicate);
             right_expr == nullptr) {
    can_be_combine = false;
  }
  if (can_be_combine) {
    return std::make_shared<LogicExpression>(left_predicate, right_predicate, LogicType::And);
  }
  return nullptr;
}

}  // namespace bustub
