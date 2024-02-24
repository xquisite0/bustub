
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
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  std::cout << "Header Max Depth: " << header_max_depth << "\n";
  std::cout << "Directory Max Depth: " << directory_max_depth << "\n";
  std::cout << "Bucket Max Size: " << bucket_max_size << "\n";
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
  // initialized_ = true;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // std::cout << "Getting value from key " << key << "\n";
  // uint32_t directory_index = HashToDirectoryIndex(hash_fn_.GetHash(key));
  // BasiPageGuard header_guard;

  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  // Grab the header page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  // Grab the directory page
  uint32_t directory_index = header_page->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index);

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // header_guard.Drop();

  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();

  // Fetch bucket page (create a new page if necessary)
  uint32_t bucket_index = directory_page->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // directory_guard.Drop();
  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  // perform the insertion
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // std::cout << "Inserting key-value pair " << key << ", " << value << "\n";
  // std::cout << header_page_id_ << "!!\n";
  // return false;

  WritePageGuard header_guard;
  if (header_page_id_ == INVALID_PAGE_ID) {
    BasicPageGuard header_guard_basic = bpm_->NewPageGuarded(&header_page_id_);
    // initialized_ = true;
    header_guard = header_guard_basic.UpgradeWrite();
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    header_page->Init(header_max_depth_);
  } else {
    // Grab the header page
    header_guard = bpm_->FetchPageWrite(header_page_id_);
  }

  // std::cout << "Header page : " << header_page_id_ << std::endl;
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  // Grab the directory page
  uint32_t directory_index = header_page->HashToDirectoryIndex(Hash(key));
  // std::cout << Hash(key) << "\n";
  // if (Hash(key) == 511) {
  //   std::cout << "here\n";
  // }
  // std::cout << "hash : " << Hash(key) << std::endl;
  // std::cout << "Directory index : " << directory_index << std::endl;

  return InsertToNewDirectory(header_page, directory_index, Hash(key), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);

  WritePageGuard directory_guard;

  if (directory_page_id == INVALID_PAGE_ID) {
    BasicPageGuard directory_guard_basic = bpm_->NewPageGuarded(&directory_page_id);
    header->SetDirectoryPageId(directory_idx, directory_page_id);
    directory_guard = directory_guard_basic.UpgradeWrite();
    auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
  } else {
    directory_guard = bpm_->FetchPageWrite(directory_page_id);
  }
  // std::cout << "Directory page id: " << directory_page_id << std::endl;
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  // std::cout << directory_page->GetGlobalDepth() << " ";

  // Fetch bucket page (create a new page if necessary)
  uint32_t bucket_index = directory_page->HashToBucketIndex(hash);
  // std::cout << key << " : " << bucket_index << "\n";

  return InsertToNewBucket(directory_page, bucket_index, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  WritePageGuard bucket_guard;
  if (bucket_page_id == INVALID_PAGE_ID) {
    // page_id_t new_bucket_page_id;
    // std::cout << "ran\n";
    BasicPageGuard bucket_guard_basic = bpm_->NewPageGuarded(&bucket_page_id);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    bucket_guard = bucket_guard_basic.UpgradeWrite();
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
  } else {
    bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  }
  // std::cout << "Bucket page: " << bucket_page_id << " | Inserting value is " << key << std::endl;
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  V value_placeholder;
  if (bucket_page->Lookup(key, value_placeholder, cmp_)) {
    return false;
  }

  // Checks whether the page is full, if so we increase the local depth
  if (bucket_page->IsFull()) {
    // double the hash table if needed

    // CHECK THIS LINE
    if (directory->GetLocalDepth(bucket_idx) == directory->GetGlobalDepth()) {
      // early return if the hash table is full.
      if (directory->Size() == directory->MaxSize()) {
        // std::cout << "RAN\n\n\n\n\n";
        return false;
      }
      if (directory->GetGlobalDepth() == directory_max_depth_) {
        return false;
      }
      // std::cout << directory_max_depth_;
      directory->IncrGlobalDepth();
    }

    // create a new bucket page
    page_id_t new_split_bucket_page_id;

    std::vector<int> belongs_to_new_bucket;
    uint32_t split_local_bits = bucket_idx & ((1 << directory->GetLocalDepth(bucket_idx)) - 1);
    // iterate through all the bucket_page_ids
    for (uint32_t i = 0; i < directory->Size(); i++) {
      uint32_t cur_local_bits = i & ((1 << directory->GetLocalDepth(i)) - 1);

      // current bucket id is involved in the splitting process
      if (cur_local_bits == split_local_bits) {
        // arbitrarily decided that if the new bit we are looking at is a 1, assign it to the n
        uint32_t one_as_new_bit = i & (1 << directory->GetLocalDepth(i));

        if (one_as_new_bit > 0) {
          belongs_to_new_bucket.push_back(i);
          // directory->SetBucketPageId(i, new_split_bucket_page_id);
        }
        directory->IncrLocalDepth(i);
      }
    }
    // if the page has the same local_depth_ bits, then they point to the same bucket
    // update local depth
    // if we have to add a 0, we keep the same bucket
    // if we have to add a 1, we point to the new bucket page.

    // look through the old bucket page, if the records belong to the new page, push them there
    uint32_t bucket_page_size = bucket_page->Size();
    std::vector<std::pair<K, V>> belongs_to_new_page;
    // std::cout << bucket_page_size << std::endl;
    for (int j = bucket_page_size - 1; j >= 0; j--) {
      // std::cout << "j: " << j << std::endl;
      if (Hash(bucket_page->KeyAt(j)) & (1 << (directory->GetLocalDepth(bucket_idx) - 1))) {
        // std::cout << "Successful insert into new bucket? "
        // new_split_bucket_page->Insert(bucket_page->KeyAt(j), bucket_page->ValueAt(j), cmp_);
        belongs_to_new_page.push_back({bucket_page->KeyAt(j), bucket_page->ValueAt(j)});
        bucket_page->RemoveAt(j);
      }
    }

    // decide if the key belongs in the current bucket page or the split bucket page
    if (Hash(key) & (1 << (directory->GetLocalDepth(bucket_idx) - 1))) {
      // possibility that all elements have shifted to new split page
      // if (new_split_bucket_page->IsFull()) {
      //   return false;
      // }
      // new_split_bucket_page->Insert(key, value, cmp_);
      belongs_to_new_page.push_back({key, value});
    } else {
      // possibility that all elements have not shifted, and remain in the original bucket page
      if (bucket_page->IsFull()) {
        return false;
      }
      bucket_page->Insert(key, value, cmp_);
    }
    bucket_guard.Drop();

    // now insert the elements into the new bucket page
    WritePageGuard new_split_bucket_page_guard = bpm_->NewPageGuarded(&new_split_bucket_page_id).UpgradeWrite();
    auto new_split_bucket_page = new_split_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_split_bucket_page->Init(bucket_max_size_);
    for (std::pair<K, V> p : belongs_to_new_page) {
      if (new_split_bucket_page->IsFull()) {
        return false;
      }
      new_split_bucket_page->Insert(p.first, p.second, cmp_);
    }
    for (int i : belongs_to_new_bucket) {
      directory->SetBucketPageId(i, new_split_bucket_page_id);
    }
  } else {
    // the bucket page is not full, insert as per normal
    if (bucket_page->IsFull()) {
      return false;
    }
    bucket_page->Insert(key, value, cmp_);
  }
  // if (bucket_page->IsFull()) {
  //   return false;
  // }
  // perform the insertion
  // std::cout << "ran\n";
  // check if the inserted key belongs in either the current bucket, or the split bucket

  // belongs to current bucket
  // if (directory->HashToBucketIndex(Hash(key)) == bucket_idx) {
  //   bucket_page->Insert(key, value, cmp_);
  // } else {
  //   page_id_t split_page_id;
  //   WritePageGuard new_split_bucket_page_guard = bpm_->FetchPageWrite(&split_page_id).UpgradeWrite();
  //   auto new_split_bucket_page = new_split_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  //   new_split_bucket_page->Insert(key, value, cmp_);
  // }
  // bucket_page->Insert(key, value, cmp_);
  // std::cout << "bucket size: " << bucket_page->Size() << std::endl;
  // PrintHT();
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // std::cout << "Removing key " << key << "\n";
  // BasicPageGuard header_guard;

  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  // Grab the header page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  // Grab the directory page
  uint32_t directory_index = header_page->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index);

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // std::cout << "ran";
  header_guard.Drop();
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_index = directory_page->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // perform the deletion

  // key not in bucket
  if (!bucket_page->Remove(key, cmp_)) {
    // std::cout << "can't find key " << key << std::endl;
    return false;
  }

  // execute the recursive merging, if needed. (bucket is empty)
  while (bucket_page->IsEmpty()) {
    if (directory_page->GetGlobalDepth() == 0) {
      break;
    }
    uint32_t split_image = directory_page->GetSplitImageIndex(bucket_index);
    if (directory_page->GetLocalDepth(bucket_index) != directory_page->GetLocalDepth(split_image)) {
      break;
    }
    uint32_t merged_bucket = directory_page->GetBucketPageId(split_image);
    // iterate through the array, for everything with the same first local depth bits as us,
    uint32_t depth_of_split_bucket = directory_page->GetLocalDepth(bucket_index);
    uint32_t matching_bits = bucket_index & (1 << ((depth_of_split_bucket - 1) - 1));
    for (uint32_t i = 0; i < directory_page->Size(); i++) {
      uint32_t cur_matching_bits = i & (1 << ((depth_of_split_bucket - 1) - 1));
      if (matching_bits == cur_matching_bits) {
        directory_page->DecrLocalDepth(i);
        directory_page->SetBucketPageId(i, merged_bucket);

        bucket_index = fmin(bucket_index, i);
      }
      // update local depth
    }
    // directory_page->SetBucketPageId(bucket_index, directory_page->GetBucketPageId(split_image));

    // update global depth as well if necessary
    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }

    // then assign bucket page stays the same
    bucket_guard.Drop();
    WritePageGuard bucket_guard_temp = bpm_->FetchPageWrite(merged_bucket);
    bucket_guard = std::move(bucket_guard_temp);
    // WritePageGuard bucket_guard = bpm_->FetchPageWrite(merged_bucket);
    bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    if (directory_page->GetGlobalDepth() == 0) {
      break;
    }
    WritePageGuard bucket_guard_split =
        bpm_->FetchPageWrite(directory_page->GetBucketPageId(directory_page->GetSplitImageIndex(bucket_index)));
    auto bucket_page_split = bucket_guard_split.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (bucket_page_split->IsEmpty()) {
      // std::cout << "RAN\n";
      bucket_page = bucket_page_split;
      bucket_index = directory_page->GetSplitImageIndex(bucket_index);
    }

    // PrintHT();
  }
  // PrintHT();
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
