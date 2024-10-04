#include "execution/executors/topn_executor.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      top_entries_(std::priority_queue<Tuple, std::vector<Tuple>, SortExecutor::Cmp>(
          {plan_->GetOrderBy(), plan_->OutputSchema()})) {}

void TopNExecutor::Init() {
  if (!if_init_) {
    if_init_ = true;
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      top_entries_.push(std::move(tuple));
      if (GetNumInHeap() == (plan_->GetN() + 1)) {
        top_entries_.pop();
      }
    }
    while (!top_entries_.empty()) {
      results_.emplace_back(top_entries_.top());
      top_entries_.pop();
    }
    SortExecutor::Cmp cmp(plan_->GetOrderBy(), plan_->OutputSchema());
    std::sort(results_.begin(), results_.end(), cmp);
  }
  results_iter_ = results_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (results_iter_ != results_.end()) {
    *tuple = Tuple(*results_iter_);
    results_iter_++;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
