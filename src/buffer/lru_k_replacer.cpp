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
#include <memory>
#include <utility>
#include "common/exception.h"
#include "fmt/format.h"
#include "storage/page/extendible_htable_directory_page.h"

namespace bustub {

void LRUKNode::Access(size_t timestamp) {
  if (history_.size() == k_) {
    history_.pop_front();
  }
  history_.push_back(timestamp);
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
  size_t max_dddistance = 0;
  bool is_smaller_k = false;
  bool if_found = false;
  frame_id_t evict_frame_id = 0;
  for (auto &pair : node_store_) {
    auto &node = pair.second;
    if (node.GetEvictable()) {
      if_found = true;
      if (node.GetHistorySize() == 0) {
        evict_frame_id = pair.first;
        break;
      }
      size_t first_history = node.GetEarlyHistory();
      size_t distance = current_timestamp_ - first_history;
      if (node.GetHistorySize() < k_ && distance > max_distance) {
        is_smaller_k = true;
        max_distance = distance;
        evict_frame_id = pair.first;
      } else if (node.GetHistorySize() == k_ && !is_smaller_k && distance > max_dddistance) {
        max_dddistance = distance;
        evict_frame_id = pair.first;
      }
    }
  }
  if (if_found) {
    *frame_id = evict_frame_id;
    node_store_.find(evict_frame_id)->second.RemoveFromReplacer();
    node_store_.erase(evict_frame_id);
    curr_size_.fetch_sub(1);
  }
  return if_found;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (!IfFrameIdValid(frame_id)) {
    throw bustub::Exception(fmt::format("FrameId {} Invalid", frame_id));
  }
  std::lock_guard<std::mutex> lock(latch_);
  std::unique_ptr<LRUKNode> new_node_ptr;
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    // Fail to find out the LRUKNode.
    new_node_ptr = std::make_unique<LRUKNode>(k_);
    node_store_.insert(std::make_pair(frame_id, *new_node_ptr));
  }
  auto &node = (iter == node_store_.end()) ? *new_node_ptr : iter->second;
  if (set_evictable && !node.GetEvictable()) {
    node.SetEvictable(set_evictable);
    curr_size_.fetch_add(1);
  }
  if (!set_evictable && node.GetEvictable()) {
    node.SetEvictable(set_evictable);
    curr_size_.fetch_sub(1);
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    // Fail to find out the LRUKNode.
    auto new_node_ptr = std::make_unique<LRUKNode>(k_);
    if (access_type != AccessType::Scan) {
      // 此处易出错先touch再加1
      new_node_ptr->Access(current_timestamp_.fetch_add(1));
    }
    node_store_.insert(std::make_pair(frame_id, *new_node_ptr));
  } else {
    auto &node = iter->second;
    if (access_type != AccessType::Scan) {
      node.Access(current_timestamp_.fetch_add(1));
    }
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
    node_store_.erase(iter);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_.load(); }

}  // namespace bustub
