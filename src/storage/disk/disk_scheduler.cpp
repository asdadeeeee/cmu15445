//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <utility>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
    writing_pages_.clear();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

void DiskScheduler::ProcessRequest(const std::shared_ptr<DiskRequest> &r) {
  // 添加正在写的pageID vector 解决写后读一致性
  while (true) {
    // 使用队列？
    wait_lock_.lock();
    if (writing_pages_.find(r->page_id_) == writing_pages_.end()) {
      wait_lock_.unlock();
      break;
    }
    wait_lock_.unlock();
  }
  wait_lock_.lock();
  if (r->is_write_) {
    writing_pages_.insert(r->page_id_);
  }
  wait_lock_.unlock();
  std::thread t([this, r]() {  // 使用智能指针复制捕获，自动管理生命周期
    if (r->is_write_) {
      disk_manager_->WritePage(r->page_id_, r->data_);
      wait_lock_.lock();
      writing_pages_.erase(r->page_id_);
      wait_lock_.unlock();
    } else {
      disk_manager_->ReadPage(r->page_id_, r->data_);
    }
    r->callback_.set_value(true);
  });
  t.detach();  // 分离新创建的线程
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request = request_queue_.Get();
    if (request.has_value()) {
      auto request_ptr = std::make_shared<DiskRequest>(std::move(*request));  // 创建智能指针
      ProcessRequest(request_ptr);                                            // 传递智能指针
    } else {
      break;
    }
  }
}

}  // namespace bustub
