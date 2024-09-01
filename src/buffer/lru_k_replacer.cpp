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
#include <utility>
#include "common/exception.h"
#include "fmt/format.h"
#include "storage/page/extendible_htable_directory_page.h"

namespace bustub {

void LRUKNode::Access(size_t timestamp) {
  if (history_.size() == k_ && !history_.empty()) {
    history_.pop_front();
  }
  history_.push_back(timestamp);
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
  return frame_id <= static_cast<int32_t>(replacer_size_) && frame_id > 0;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  node_store_.reserve(num_frames);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  size_t max_distance = 0;
  size_t min_history_size = k_;
  bool if_found = false;
  frame_id_t evict_frame_id;
  for (auto pair : node_store_) {
    if (pair.second.GetEvictable()) {
      if_found = true;
      size_t distance = current_timestamp_ - pair.second.GetEarlyHistory();
      if (pair.second.GetHistorySize() < min_history_size) {
        min_history_size = pair.second.GetHistorySize();
        max_distance = distance;
        evict_frame_id = pair.first;
      } else if (pair.second.GetHistorySize() == min_history_size && distance > max_distance) {
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

  return if_found;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (!IfFrameIdValid(frame_id)) {
    throw bustub::Exception(fmt::format("FrameId {} Invalid", frame_id));
  }
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_.fetch_add(1);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    LRUKNode new_node{current_timestamp_, k_};
    node_store_.emplace(frame_id, std::move(new_node));
  } else {
    iter->second.Access(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (!IfFrameIdValid(frame_id)) {
    throw bustub::Exception(fmt::format("FrameId {} Invalid", frame_id));
  }
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (iter->second.GetEvictable() == false && set_evictable == true) {
      curr_size_.fetch_add(1);
    } else if (iter->second.GetEvictable() == true && set_evictable == false) {
      curr_size_.fetch_sub(1);
    }
    iter->second.SetEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (!IfFrameIdValid(frame_id)) {
    throw bustub::Exception(fmt::format("FrameId {} Invalid", frame_id));
  }
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (!iter->second.GetEvictable()) {
      throw bustub::Exception(fmt::format("FrameId {} not evictable", frame_id));
    }
    iter->second.RemoveFromReplacer();
    curr_size_.fetch_sub(1);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_.load(); }

}  // namespace bustub
