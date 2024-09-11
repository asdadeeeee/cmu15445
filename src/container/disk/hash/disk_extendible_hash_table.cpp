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
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  uint32_t bucket_page_idx = 0;
  if (!LookupBucketPageId(key, bucket_page_id, bucket_page_idx, transaction)) {
    return false;
  }
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
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  uint32_t directory_index = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (directory_page_id == INVALID_PAGE_ID) {
    // 升级后原先guard无法使用，此guard需要保持在if块内作用
    WritePageGuard header_guard_write = header_guard.UpgradeWrite();
    auto header_page_write = header_guard_write.AsMut<ExtendibleHTableHeaderPage>();
    return InsertToNewDirectory(header_page_write, directory_index, hash, key, value);
  }
  // 离开if后 原先header_page可能被换出，不应使用
  BasicPageGuard directory_guard = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    WritePageGuard directory_guard_write = directory_guard.UpgradeWrite();
    auto directory_page_write = directory_guard_write.AsMut<ExtendibleHTableDirectoryPage>();
    return InsertToNewBucket(directory_page_write, bucket_index, key, value);
  }
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // 桶已满需要分裂；
  if (bucket_page->IsFull()) {
    WritePageGuard directory_guard_write = directory_guard.UpgradeWrite();
    auto directory_page_write = directory_guard_write.AsMut<ExtendibleHTableDirectoryPage>();
    uint32_t old_local_depth = directory_page->GetLocalDepth(bucket_index);
    // 有可能增加global depth
    if (old_local_depth == directory_page->GetGlobalDepth()) {
      if (directory_page_write->GetGlobalDepth() < directory_page_write->GetMaxDepth()) {
        directory_page_write->IncrGlobalDepth();
      } else {
        return false;
      }
    }
    uint32_t new_bucket_idx = directory_page->GetActSplitIndex(bucket_index);
    // 理论上UpdateDirectoryMapping会设置此处local depth ，此处为了方便获取新local_depth_mask供下方调用
    directory_page_write->IncrLocalDepth(bucket_index);
    auto new_local_depth_mask = directory_page_write->GetLocalDepthMask(bucket_index);

    if (!IncrNewBucket(directory_page_write, new_bucket_idx)) {
      return false;
    }
    page_id_t new_bucket_page_id = directory_page->GetBucketPageId(new_bucket_idx);
    UpdateDirectoryMapping(directory_page_write, new_bucket_idx, new_bucket_page_id, old_local_depth + 1,
                           new_local_depth_mask);
    // 移动原bucket中应该属于新bucket的k-v
    WritePageGuard new_bucket_guard_write = bpm_->FetchPageWrite(new_bucket_page_id);
    auto new_bucket_page = new_bucket_guard_write.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    MigrateEntries(bucket_page, new_bucket_page, new_bucket_idx, new_local_depth_mask);

    // 插入的值有可能在原idx也有可能在新idx（global depth变化）
    uint32_t insert_bucket_idx = directory_page_write->HashToBucketIndex(hash);
    if (insert_bucket_idx == new_bucket_idx) {
      if (!new_bucket_page->Insert(key, value, cmp_)) {
        return false;
      }
    } else if (insert_bucket_idx == bucket_index) {
      if (!bucket_page->Insert(key, value, cmp_)) {
        return false;
      }
    } else {
      throw Exception("should not be here");
    }
    return true;
  }
  // 未满则直接插入
  return bucket_page->Insert(key, value, cmp_);
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
  if (!IncrNewBucket(directory, bucket_idx)) {
    return false;
  }
  WritePageGuard new_bucket_guard_write = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_idx));
  auto new_bucket_page = new_bucket_guard_write.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  return new_bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::IncrNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx)
    -> bool {
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  if (new_bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto new_bucket_guard_write = new_bucket_guard.UpgradeWrite();
  auto new_bucket = new_bucket_guard_write.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
  new_bucket->Init(bucket_max_size_);
  return true;
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
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
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
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  uint32_t bucket_page_idx = 0;
  if (!LookupBucketPageId(key, bucket_page_id, bucket_page_idx, transaction)) {
    return false;
  }
  // todo 此处优化 先读锁 若查找到则升级写锁
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  V v;
  uint32_t idx = 0;
  if (!bucket_page->LookupAt(key, v, cmp_, idx)) {
    return false;
  }
  bucket_page->RemoveAt(idx);
  if (bucket_page->IsEmpty()) {
    page_id_t directory_page_id = INVALID_PAGE_ID;
    uint32_t directory_page_idx = 0;
    if (!LookupDirectoryPageId(key, directory_page_id, directory_page_idx, transaction)) {
      return false;
    }
    WritePageGuard directory_guard_write = bpm_->FetchPageWrite(directory_page_id);
    auto directory = directory_guard_write.AsMut<ExtendibleHTableDirectoryPage>();
    if (directory->GetLocalDepth(bucket_page_idx) == 0) {
      return true;
    }
    directory->DecrLocalDepth(bucket_page_idx);
    uint32_t split_bucket_idx = directory->GetActSplitIndex(bucket_page_idx);
    auto src_local_depth_mask = directory->GetLocalDepthMask(split_bucket_idx);
    auto split_bucket_page_id = directory->GetBucketPageId(split_bucket_idx);
    UpdateDirectoryMapping(directory, bucket_page_idx, split_bucket_page_id, directory->GetLocalDepth(bucket_page_idx),
                           src_local_depth_mask);
    UpdateDirectoryMapping(directory, split_bucket_idx, split_bucket_page_id, directory->GetLocalDepth(bucket_page_idx),
                           src_local_depth_mask);
    // while (directory->CanShrink()) {
    //   directory->DecrGlobalDepth();
    // }
    if (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::LookupBucketPageId(const K &key, page_id_t &bucket_page_id,
                                                           uint32_t &bucket_page_idx, Transaction *transaction) const
    -> bool {
  page_id_t directory_page_id = INVALID_PAGE_ID;
  uint32_t directory_page_idx = 0;
  uint32_t hash = Hash(key);
  if (!LookupDirectoryPageId(key, directory_page_id, directory_page_idx, transaction)) {
    return false;
  }
  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  auto tmp_bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash));
  if (tmp_bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  bucket_page_id = tmp_bucket_page_id;
  bucket_page_idx = directory_page->HashToBucketIndex(hash);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::LookupDirectoryPageId(const K &key, page_id_t &directory_page_id,
                                                              uint32_t &directory_page_idx,
                                                              Transaction *transaction) const -> bool {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto tmp_directory_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  if (tmp_directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_page_id = tmp_directory_page_id;
  directory_page_idx = header_page->HashToDirectoryIndex(hash);
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
