#include <cstddef>
#include <cstdint>
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_per_group_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/window_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit && optimized_plan->GetChildren().size() == 1 &&
      optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
    const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*(optimized_plan->GetChildAt(0)));
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0), sort_plan.order_bys_,
                                          limit_plan.limit_);
  }

  if (optimized_plan->GetType() == PlanType::Filter && optimized_plan->GetChildren().size() == 1 &&
      optimized_plan->GetChildAt(0)->GetType() == PlanType::Window) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    const auto &window_plan = dynamic_cast<const WindowFunctionPlanNode &>(*(optimized_plan->GetChildAt(0)));
    if (window_plan.window_functions_.size() == 1 &&
        window_plan.window_functions_.begin()->second.type_ == WindowFunctionType::Rank) {
      if (auto compare_expr = std::dynamic_pointer_cast<ComparisonExpression>(filter_plan.GetPredicate());
          compare_expr != nullptr) {
        if (compare_expr->comp_type_ == ComparisonType::LessThanOrEqual) {
          if (auto left_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->GetChildAt(0));
              left_expr != nullptr) {
            if (auto right_expr = std::dynamic_pointer_cast<ConstantValueExpression>(compare_expr->GetChildAt(1));
                right_expr != nullptr) {
              if (left_expr->GetColIdx() == window_plan.window_functions_.begin()->first &&
                  right_expr->val_.CheckInteger()) {
                auto n = right_expr->val_.GetAs<int32_t>();
                return std::make_shared<TopNPerGroupPlanNode>(
                    window_plan.output_schema_, window_plan.GetChildAt(0), window_plan.columns_,
                    window_plan.window_functions_.begin()->second.partition_by_,
                    window_plan.window_functions_.begin()->second.order_by_, static_cast<std::size_t>(n));
              }
            }
          }
        }

        if (auto left_expr = std::dynamic_pointer_cast<ConstantValueExpression>(compare_expr->GetChildAt(0));
            left_expr != nullptr) {
          if (compare_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
            if (auto right_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->GetChildAt(1));
                right_expr != nullptr) {
              if (right_expr->GetColIdx() == window_plan.window_functions_.begin()->first &&
                  left_expr->val_.CheckInteger()) {
                auto n = left_expr->val_.GetAs<size_t>();
                return std::make_shared<TopNPerGroupPlanNode>(
                    window_plan.output_schema_, window_plan.GetChildAt(0), window_plan.columns_,
                    window_plan.window_functions_.begin()->second.partition_by_,
                    window_plan.window_functions_.begin()->second.order_by_, static_cast<std::size_t>(n));
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}
}  // namespace bustub
