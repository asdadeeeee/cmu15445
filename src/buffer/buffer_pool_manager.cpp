//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <optional>
#include <utility>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// auto BufferPoolManager::FindPageIdByFrameId(frame_id_t frame_id) -> std::optional<page_id_t>
// {
//   for(auto pair : page_table_)
//   {
//     if(pair.second == frame_id)
//     {
//       return pair.first;
//     }
//   }
//   return std::nullopt;
// }
auto BufferPoolManager::GetPreparedFrame(frame_id_t *prepared_frame_id) -> bool {
  bool if_found = false;
  if (!free_list_.empty()) {
    *prepared_frame_id = free_list_.front();
    free_list_.pop_front();
    if_found = true;
  } else if (replacer_->Evict(prepared_frame_id)) {
    if (pages_[*prepared_frame_id].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({/*is_write=*/true, pages_[*prepared_frame_id].GetData(),
                                 pages_[*prepared_frame_id].GetPageId(), std::move(promise)});
      if (!future.get()) {
        throw Exception("FetchPage Schedule failed");
      }
      pages_[*prepared_frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[*prepared_frame_id].GetPageId());
    if_found = true;
  }
  return if_found;
}
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // todo 可优化
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t prepared_frame_id = 0;
  if (!GetPreparedFrame(&prepared_frame_id)) {
    return nullptr;
  }
  page_id_t new_page_id = AllocatePage();
  // todo 可以这里放锁
  replacer_->SetEvictable(prepared_frame_id, false);
  pages_[prepared_frame_id].pin_count_++;

  replacer_->RecordAccess(prepared_frame_id);
  pages_[prepared_frame_id].ResetMemory();
  pages_[prepared_frame_id].page_id_ = new_page_id;
  page_table_.emplace(new_page_id, prepared_frame_id);
  *page_id = new_page_id;
  return &pages_[prepared_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    throw Exception("INVALID_PAGE_ID");
  }
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  frame_id_t prepared_frame_id = 0;
  bool if_already_in_pool = false;
  if (iter != page_table_.end()) {
    prepared_frame_id = iter->second;
    if_already_in_pool = true;
  } else if (!GetPreparedFrame(&prepared_frame_id)) {
    return nullptr;
  }
  if (!if_already_in_pool) {
    pages_[prepared_frame_id].page_id_ = page_id;
    page_table_.emplace(page_id, prepared_frame_id);
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({/*is_write=*/false, pages_[prepared_frame_id].GetData(),
                               pages_[prepared_frame_id].GetPageId(), std::move(promise)});
    if (!future.get()) {
      throw Exception("FetchPage Schedule failed");
    }
    if (pages_[prepared_frame_id].is_dirty_) {
      throw Exception("should not be dirty");
    }
  }
  replacer_->SetEvictable(prepared_frame_id, false);
  pages_[prepared_frame_id].pin_count_++;

  replacer_->RecordAccess(prepared_frame_id);

  return &pages_[prepared_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);

  if (iter != page_table_.end() && pages_[iter->second].GetPinCount() > 0) {
    frame_id_t frame_id = iter->second;
    if (!pages_[frame_id].IsDirty()) {
      pages_[frame_id].is_dirty_ = is_dirty;
    }
    pages_[frame_id].pin_count_--;
    if (pages_[frame_id].GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frame_id = iter->second;
    if (pages_[frame_id].GetPageId() != page_id) {
      throw Exception("should be equal");
    }
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(
        {/*is_write=*/true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
    if (!future.get()) {
      throw Exception("FetchPage Schedule failed");
    }
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  for (auto pair : page_table_) {
    if (!FlushPage(pair.first)) {
      throw Exception("FlushAllPages failed");
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frame_id = iter->second;
    if (pages_[frame_id].GetPinCount() > 0) {
      return false;
    }
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    replacer_->Remove(frame_id);
    free_list_.emplace_back(frame_id);
    DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
