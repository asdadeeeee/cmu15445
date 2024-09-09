//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"
#include <cstdint>
#include <cstring>

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  if (max_depth > HTABLE_HEADER_MAX_DEPTH) {
    throw Exception("overflow ExtendibleHTableHeaderPage HTABLE_HEADER_MAX_DEPTH");
  }
  max_depth_ = max_depth;
  std::memset(directory_page_ids_, -1, sizeof(directory_page_ids_));
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  return hash >> (32 - max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  if (directory_idx >= MaxSize()) {
    throw Exception("overflow ExtendibleHTableHeaderPage MaxSize");
  }
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  if (directory_idx >= MaxSize()) {
    throw Exception("overflow ExtendibleHTableHeaderPage MaxSize");
  }
  directory_page_ids_[directory_idx] = directory_page_id;
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

}  // namespace bustub
