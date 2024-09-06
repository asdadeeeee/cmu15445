//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <cstdio>
#include <list>
#include <utility>
#include "common/exception.h"
#include "fmt/format.h"
#include "storage/page/extendible_htable_directory_page.h"

namespace bustub {

void LRUKNode::Access(size_t timestamp, AccessType access_type) {
  if (access_type != AccessType::Scan) {
    if (history_.size() == k_ && !history_.empty()) {
      history_.pop_front();
    }
    history_.push_back(timestamp);
  }
}

void LRUKNode::PrintHistory() {
  if (!history_.empty()) {
    std::list<size_t>::iterator iter;
    for (iter = history_.begin(); iter != history_.end(); iter++) {
      printf(" %lu ", *iter);
    }
  }
  printf("\n");
}

void LRUKNode::RemoveFromReplacer() {
  history_.clear();
  is_evictable_ = false;
}

auto LRUKNode::GetEarlyHistory() -> size_t {
  if (!history_.empty()) {
    return history_.front();
  }
  return 0;
}

auto LRUKReplacer::IfFrameIdValid(const frame_id_t frame_id) -> bool {
  return frame_id < static_cast<int32_t>(replacer_size_) && frame_id >= 0;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  node_store_.reserve(num_frames);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (Size() == 0) {
    return false;
  }
  size_t max_distance = 0;
  size_t early_history = 0xFFFFFFFF;
  bool if_found = false;
  frame_id_t evict_frame_id = 0;
  for (auto pair : node_store_) {
    if (pair.second.GetEvictable()) {
      if_found = true;
      size_t first_history = pair.second.GetEarlyHistory();
      size_t distance = current_timestamp_ - first_history;
      if (pair.second.GetHistorySize() < k_ && first_history < early_history) {
        early_history = first_history;
        max_distance = 0xFFFFFFFF;
        evict_frame_id = pair.first;
      } else if (pair.second.GetHistorySize() == k_ && distance > max_distance) {
        max_distance = distance;
        evict_frame_id = pair.first;
      }
    }
  }
  if (if_found) {
    *frame_id = evict_frame_id;
    auto frame = node_store_.find(evict_frame_id);
    frame->second.RemoveFromReplacer();
    curr_size_.fetch_sub(1);
  }
  // if (if_found) {
  //   printf("after Evict frame id = %d size = %zu\n", *frame_id, Size());
  // } else {
  //   printf("after fail Evict size = %zu\n", Size());
  // }
  return if_found;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  if (!IfFrameIdValid(frame_id)) {
    throw bustub::Exception(fmt::format("FrameId {} Invalid", frame_id));
  }
  std::lock_guard<std::mutex> lock(latch_);
  // printf("RecordAccess frame id = %d \n", frame_id);
  current_timestamp_.fetch_add(1);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    LRUKNode new_node{current_timestamp_, k_};
    // printf("after RecordAccess frame id = %d ,History = ", frame_id);
    // new_node.PrintHistory();
    node_store_.emplace(frame_id, std::move(new_node));
  } else {
    iter->second.Access(current_timestamp_, access_type);
    // printf("after RecordAccess frame id = %d ,History = ", frame_id);
    // iter->second.PrintHistory();
  }
  // printf("after RecordAccess frame id = %d size = %zu\n", frame_id, Size());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (!IfFrameIdValid(frame_id)) {
    throw bustub::Exception(fmt::format("FrameId {} Invalid", frame_id));
  }
  std::lock_guard<std::mutex> lock(latch_);
  // printf("SetEvictable frame id = %d  evictable=%d\n", frame_id, static_cast<int>(set_evictable));
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (!iter->second.GetEvictable() && set_evictable) {
      curr_size_.fetch_add(1);
    } else if (iter->second.GetEvictable() && !set_evictable) {
      curr_size_.fetch_sub(1);
    }
    iter->second.SetEvictable(set_evictable);
  }
  // printf("after SetEvictable frame id = %d size = %zu\n", frame_id, Size());
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (!IfFrameIdValid(frame_id)) {
    throw bustub::Exception(fmt::format("FrameId {} Invalid", frame_id));
  }
  std::lock_guard<std::mutex> lock(latch_);
  // printf("remove frame id = %d \n", frame_id);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (!iter->second.GetEvictable()) {
      throw bustub::Exception(fmt::format("FrameId {} not evictable", frame_id));
    }
    iter->second.RemoveFromReplacer();
    curr_size_.fetch_sub(1);
    node_store_.erase(iter);
  }
  // printf("after remove frame id = %d size = %zu\n", frame_id, Size());
}

auto LRUKReplacer::Size() -> size_t { return curr_size_.load(); }

}  // namespace bustub
