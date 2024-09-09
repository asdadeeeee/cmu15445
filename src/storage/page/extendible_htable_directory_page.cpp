//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <unordered_map>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  if (max_depth > HTABLE_DIRECTORY_MAX_DEPTH) {
    throw Exception("over flow");
  }
  max_depth_ = max_depth;
  global_depth_ = 0;
  memset(local_depths_, 0, sizeof(local_depths_));
  memset(bucket_page_ids_, -1, sizeof(bucket_page_ids_));
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  return (1 << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  uint32_t global_mask = GetGlobalDepthMask();
  // uint32_t local_depth_idx =  hash & global_mask;
  // uint32_t local_mask = GetLocalDepthMask(local_depth_idx);
  return hash & global_mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= Size()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= Size()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  // uint32_t local_depth = GetLocalDepth(bucket_idx);
  return bucket_idx ^ (1 << (GetGlobalDepth() - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ == GetMaxDepth()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage GetMaxDepth()");
  }
  if (global_depth_ < GetMaxDepth()) {
    global_depth_++;
    for (uint32_t i = 0; i < Size(); i++) {
      auto split_bucket_idx = GetSplitImageIndex(i);
      local_depths_[split_bucket_idx] = local_depths_[i];
      bucket_page_ids_[split_bucket_idx] = bucket_page_ids_[i];
    }
  }

  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    throw Exception("minimum global depth");
  }
  if (global_depth_ > 0) {
    global_depth_--;
  }
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  bool can_shrink = false;
  for (auto local_depth : local_depths_) {
    if (local_depth == GetGlobalDepth()) {
      can_shrink = false;
      break;
    }
    if (local_depth > GetGlobalDepth()) {
      throw Exception("should not be here");
    }
    can_shrink = true;
  }
  return can_shrink;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << GetGlobalDepth(); }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << GetMaxDepth(); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx >= Size() || local_depth > GetMaxDepth()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  // if(local_depth > GetGlobalDepth())
  // {
  //   global_depth_ = local_depth;
  // }
  local_depths_[bucket_idx] = local_depth;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  // if(GetLocalDepth(bucket_idx) == GetGlobalDepth())
  // {
  //   IncrGlobalDepth();
  // }
  local_depths_[bucket_idx]++;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size() || local_depths_[bucket_idx] <= 0) {
    throw Exception("overflow ExtendibleHTableDirectoryPage Size");
  }
  local_depths_[bucket_idx]--;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

}  // namespace bustub
