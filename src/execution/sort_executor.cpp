#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  if (!if_init_) {
    if_init_ = true;
    child_executor_->Init();
    Tuple child_tuple;
    RID child_rid;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      child_tuples_.emplace_back(std::move(child_tuple));
    }
    Cmp cmp(plan_->GetOrderBy(), plan_->OutputSchema());
    std::sort(child_tuples_.begin(), child_tuples_.end(), cmp);
  }

  child_tuples_iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_tuples_iter_ != child_tuples_.end()) {
    *tuple = Tuple(*child_tuples_iter_);
    child_tuples_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
