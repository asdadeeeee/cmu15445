#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.Clear();
  // that.Drop();
}

void BasicPageGuard::Clear() {
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    // if(!bpm_->UnpinPage(PageId(), is_dirty_))
    // {
    //   printf("zzj errot\n");
    //   throw Exception("hh");
    // }
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  Clear();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.Clear();
  // that.Drop();
  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  this->page_->RLatch();
  ReadPageGuard read_page_guard(this->bpm_, this->page_);
  Clear();
  return read_page_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  this->page_->WLatch();
  WritePageGuard write_page_guard(this->bpm_, this->page_);
  Clear();
  return write_page_guard;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  this->guard_ = std::move(that.guard_);
  // that.Drop();
  // if (this->guard_.page_ != nullptr) {
  //   this->guard_.page_->RLatch();
  // }
  // that.guard_.Clear();
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  // that.Drop();
  // if (this->guard_.page_ != nullptr) {
  //   this->guard_.page_->RLatch();
  // }
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.bpm_->UnpinPage(this->guard_.PageId(), this->guard_.is_dirty_);
    this->guard_.page_->RUnlatch();
  }
  // this->guard_.Drop();
  this->guard_.Clear();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  this->guard_ = std::move(that.guard_);
  // that.Drop();
  // if (this->guard_.page_ != nullptr) {
  //   this->guard_.page_->WLatch();
  // }
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  // that.Drop();
  // if (this->guard_.page_ != nullptr) {
  //   this->guard_.page_->WLatch();
  // }
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.bpm_->UnpinPage(this->guard_.PageId(), this->guard_.is_dirty_);
    this->guard_.page_->WUnlatch();
  }
  this->guard_.Clear();
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
