//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  max_size_ = max_size;
  size_ = 0;
  // size_ = 0;
  // if (!initialized_) {
  //   size_ = 0;
  //   initialized_ = true;
  // }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      value = array_[i].second;
      return true;
    }
  }
  // std::cout << key
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  // std::cout << "Size: " << size_ << " | max_size : " << max_size_ << std::endl;
  if (size_ >= max_size_) {
    // std::cout << "RAn\n";
    // std::cout << "RAN\n\n";
    // std::cout << "Size: " << size_ << " | max_size : " << max_size_ << std::endl;
    return false;
  }
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      // std::cout << key << "\n";
      // std::cout << "RAN2\n\n\n";
      return false;
    }
  }
  // typedef K KeyType;
  // typedef V ValueType;
  // if (size_ >= HTableBucketArraySize(sizeof(MappingType))) {
  //   return false;
  // }
  // auto acc = array_[size_];
  // std::cout << acc.first << " " << acc.second << "\n";

  // if (key.ToString() == 510) {

  // }
  array_[size_] = {key, value};
  size_++;
  // std::cout << "New size : " << size_ << std::endl;
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  // std::cout << size_ << std::endl;
  // for (uint32_t i = 0; i < size_; i++) {
  //   std::cout << array_[i].first;
  // }
  if (size_ == 0) {
    return false;
  }
  bool found = false;
  for (uint32_t i = 0; i < size_ - 1; i++) {
    if (cmp(array_[i].first, key) == 0) {
      found = true;
    }
    if (found) {
      array_[i] = array_[i + 1];
    }
  }
  bool removed = found || (cmp(array_[size_ - 1].first, key) == 0);
  if (removed) {
    size_--;
  }
  return removed;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  if (size_ == 0) {
    return;
  }
  for (uint32_t i = bucket_idx; i < size_ - 1; i++) {
    array_[i] = array_[i + 1];
  }
  size_--;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  // std::cout << bucket_idx << std::endl;
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
