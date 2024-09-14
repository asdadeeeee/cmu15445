//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "concurrency/transaction.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  this->index_name_ = name;
  BasicPageGuard header_guard = bpm->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  uint32_t bucket_page_idx = 0;
  page_id_t directory_page_id = INVALID_PAGE_ID;
  uint32_t directory_page_idx = 0;
  uint32_t hash = Hash(key);
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  directory_page_idx = header_page->HashToDirectoryIndex(hash);
  directory_page_id = header_page->GetDirectoryPageId(directory_page_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 获得directory_page_id header可弃
  // header_guard.Drop();
  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  bucket_page_idx = directory_page->HashToBucketIndex(hash);
  bucket_page_id = directory_page->GetBucketPageId(bucket_page_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 获得bucket_page_id directory可弃
  // directory_guard.Drop();
  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V v;
  if (!bucket_page->Lookup(key, v, cmp_)) {
    return false;
  }
  result->emplace_back(std::move(v));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // 不一定需要 后续insert时会检查
  std::vector<V> values;
  if (GetValue(key, &values)) {
    return false;
  }
  // 是否申请readguard  后续添加读升级写upgrade
  // BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  uint32_t directory_index = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (directory_page_id == INVALID_PAGE_ID) {
    // 升级后原先guard无法使用，此guard需要保持在if块内作用
    // WritePageGuard header_guard_write = header_guard.UpgradeWrite();
    // auto header_page_write = header_guard_write.AsMut<ExtendibleHTableHeaderPage>();
    // return InsertToNewDirectory(header_page_write, directory_index, hash, key, value);
    return InsertToNewDirectory(header_page, directory_index, hash, key, value);
  }
  // 获得directory_page_id header可弃
  header_guard.Drop();

  // 离开if后 原先header_page可能被换出，不应使用
  // BasicPageGuard directory_guard = bpm_->FetchPageBasic(directory_page_id);
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    // WritePageGuard directory_guard_write = directory_guard.UpgradeWrite();
    // auto directory_page_write = directory_guard_write.AsMut<ExtendibleHTableDirectoryPage>();
    // return InsertToNewBucket(directory_page_write, bucket_index, key, value);
    return InsertToNewBucket(directory_page, bucket_index, key, value);
  }
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool insert_success = false;
  if (!bucket_page->IsFull()) {
    // 未满则直接插入
    insert_success = bucket_page->Insert(key, value, cmp_);
    return insert_success;
  }
  // 桶已满需要分裂；
  while (bucket_page->IsFull() && !insert_success) {
    uint32_t old_local_depth = directory_page->GetLocalDepth(bucket_index);
    // 有可能增加global depth
    if (old_local_depth == directory_page->GetGlobalDepth()) {
      if (directory_page->GetGlobalDepth() < directory_page->GetMaxDepth()) {
        directory_page->IncrGlobalDepth();
      } else {
        return false;
      }
    }
    directory_page->IncrLocalDepth(bucket_index);
    uint32_t new_bucket_idx = directory_page->GetActSplitIndex(bucket_index);

    auto new_local_depth_mask = directory_page->GetLocalDepthMask(bucket_index);

    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
    if (new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    auto new_bucket_guard_write = new_bucket_guard.UpgradeWrite();
    auto new_bucket = new_bucket_guard_write.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    directory_page->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
    new_bucket->Init(bucket_max_size_);

    UpdateDirectoryMapping(directory_page, new_bucket_idx, new_bucket_page_id, old_local_depth + 1,
                           new_local_depth_mask);
    // 移动原bucket中应该属于新bucket的k-v
    MigrateEntries(bucket_page, new_bucket, new_bucket_idx, new_local_depth_mask);

    // 插入的值有可能在原idx也有可能在新idx（global depth变化）
    uint32_t insert_bucket_idx = directory_page->HashToBucketIndex(hash);
    if (insert_bucket_idx == new_bucket_idx) {
      insert_success = new_bucket->Insert(key, value, cmp_);
      if (!insert_success && new_bucket->IsFull()) {
        bucket_guard = std::move(new_bucket_guard_write);
        bucket_page_id = new_bucket_page_id;
        bucket_index = new_bucket_idx;
        bucket_page = new_bucket;
      }
    } else if (insert_bucket_idx == bucket_index) {
      insert_success = bucket_page->Insert(key, value, cmp_);
    } else {
      throw Exception("should not be here");
    }
  }
  return insert_success;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t new_directory_page_id = INVALID_PAGE_ID;
  BasicPageGuard new_directory_guard = bpm_->NewPageGuarded(&new_directory_page_id);
  if (new_directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto new_directory_guard_write = new_directory_guard.UpgradeWrite();
  auto new_directory = new_directory_guard_write.AsMut<ExtendibleHTableDirectoryPage>();
  header->SetDirectoryPageId(directory_idx, new_directory_page_id);
  new_directory->Init(directory_max_depth_);
  uint32_t bucket_idx = new_directory->HashToBucketIndex(hash);
  return InsertToNewBucket(new_directory, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  if (new_bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto new_bucket_guard_write = new_bucket_guard.UpgradeWrite();
  auto new_bucket = new_bucket_guard_write.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
  new_bucket->Init(bucket_max_size_);
  bool flag = new_bucket->Insert(key, value, cmp_);
  return flag;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < old_bucket->Size(); i++) {
    auto pair = old_bucket->EntryAt(i);
    uint32_t hash = Hash(pair.first);
    if ((hash & local_depth_mask) == new_bucket_idx) {
      if (!new_bucket->Insert(pair.first, pair.second, cmp_)) {
        throw Exception("should not be here");
      }
      old_bucket->RemoveAt(i);
      // 很重要 此处易出bug
      i--;
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
  page_id_t old_page_id = directory->GetBucketPageId(new_bucket_idx);
  for (uint32_t i = 0; i < directory->Size(); i++) {
    if (directory->GetBucketPageId(i) == old_page_id) {
      directory->SetLocalDepth(i, new_local_depth);
      if ((i & local_depth_mask) == new_bucket_idx) {
        directory->SetBucketPageId(i, new_bucket_page_id);
      }
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  uint32_t bucket_page_idx = 0;
  // if (!LookupBucketPageId(key, bucket_page_id, bucket_page_idx, transaction)) {
  //   return false;
  // }
  // todo 此处优化 先读锁 若查找到则升级写锁
  page_id_t directory_page_id = INVALID_PAGE_ID;
  uint32_t directory_page_idx = 0;
  uint32_t hash = Hash(key);
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  directory_page_idx = header_page->HashToDirectoryIndex(hash);
  directory_page_id = header_page->GetDirectoryPageId(directory_page_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 获得directory_page_id header可弃
  // header_guard.Drop();

  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  bucket_page_idx = directory_page->HashToBucketIndex(hash);
  bucket_page_id = directory_page->GetBucketPageId(bucket_page_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool remove_flag = bucket_page->Remove(key, cmp_);
  if (!remove_flag) {
    return false;
  }
  while (bucket_page->IsEmpty()) {
    bucket_guard.Drop();
    if (directory_page->GetLocalDepth(bucket_page_idx) == 0) {
      return true;
    }
    uint32_t split_bucket_idx = directory_page->GetActSplitIndex(bucket_page_idx);
    if (directory_page->GetLocalDepth(split_bucket_idx) != directory_page->GetLocalDepth(bucket_page_idx)) {
      break;
    }
    auto src_local_depth_mask = directory_page->GetLocalDepthMask(split_bucket_idx);
    auto split_bucket_page_id = directory_page->GetBucketPageId(split_bucket_idx);
    if (split_bucket_page_id == INVALID_PAGE_ID) {
      break;
    }
    directory_page->DecrLocalDepth(bucket_page_idx);
    UpdateDirectoryMapping(directory_page, bucket_page_idx, split_bucket_page_id,
                           directory_page->GetLocalDepth(bucket_page_idx), src_local_depth_mask);
    UpdateDirectoryMapping(directory_page, split_bucket_idx, split_bucket_page_id,
                           directory_page->GetLocalDepth(bucket_page_idx), src_local_depth_mask);
    auto temp_bucket_page_id = bucket_page_id;
    bucket_page_idx = std::min(split_bucket_idx, bucket_page_idx);
    uint32_t split_img_idx = directory_page->GetActSplitIndex(bucket_page_idx);
    page_id_t split_img_id = directory_page->GetBucketPageId(split_img_idx);
    bucket_page_id = split_img_id;
    WritePageGuard split_bucket_guard = bpm_->FetchPageWrite(split_img_id);
    bucket_guard = std::move(split_bucket_guard);
    bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bpm_->DeletePage(temp_bucket_page_id);
    while (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
  }
  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
