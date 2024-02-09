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
#include <climits>
#include <cmath>
#include <stdexcept>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // std::cout << frame_id << "---------------\n";
  latch_.lock();
  frame_id_t to_evict = -1;
  size_t earliest_access = LLONG_MAX;
  size_t earliest_timestamp = LLONG_MAX;
  for (auto &frame_to_node : node_store_) {
    frame_id_t cur_frame_id = frame_to_node.first;
    LRUKNode node = frame_to_node.second;
    // std::cout << "We are now looking at frame " << cur_frame_id << "\n";
    if (!node.GetIsEvictable() || node.GetHistory().empty()) {
      // std::cout << "skipping node " << cur_frame_id << "\n";
      // std::cout << "history size : " << node.GetHistory().size() << "\n";
      continue;
    }
    // std::cout << cur_frame_id << " passed\n";
    size_t kth_access;
    if (node.GetHistory().size() < k_) {
      kth_access = 0;
    } else {
      kth_access = node.GetHistory().front();
    }
    // std::cout << "processing node " << cur_frame_id << " with first access at " << node.GetHistory().front() << " and
    // accessed " << node.GetHistory().size() << " times with kth_access as "<< kth_access << "\n";
    if (kth_access < earliest_access) {
      to_evict = cur_frame_id;
      earliest_access = kth_access;
    } else if (kth_access == earliest_access && kth_access == 0) {
      if (node.GetEarliest() < earliest_timestamp) {
        to_evict = cur_frame_id;
      }
    }
    if (kth_access == 0) {
      earliest_timestamp = fmin(earliest_timestamp, node.GetEarliest());
    }
  }
  // std::cout << to_evict << " is being evicted\n";
  // std::cout << "evicting: " << to_evict << "\n";
  if (to_evict != -1) {
    curr_size_--;
    node_store_[to_evict].ClearHistory();
    node_store_[to_evict].SetIsEvictable(false);
    // std::cout << "Here\n";
    // std::cout << (frame_id == nullptr) << "\n";
    *frame_id = to_evict;
    // std::cout << "Here2\n";
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    latch_.unlock();
    throw std::runtime_error("Invalid frame id");
  }
  node_store_[frame_id].SetK(k_);
  if (node_store_[frame_id].GetHistory().empty()) {
    node_store_[frame_id].SetEarliest(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
  }
  node_store_[frame_id].AppendHistory(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count());
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    latch_.unlock();
    throw std::runtime_error("Invalid frame id");
  }
  if (node_store_[frame_id].GetIsEvictable()) {
    if (!set_evictable) {
      curr_size_--;
      node_store_[frame_id].SetIsEvictable(set_evictable);
    }
  } else {
    if (set_evictable) {
      curr_size_++;
      node_store_[frame_id].SetIsEvictable(set_evictable);
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (!node_store_[frame_id].GetIsEvictable()) {
    latch_.unlock();
    throw std::runtime_error("Non-evictable frame");
  }
  if (node_store_[frame_id].GetHistory().empty()) {
    latch_.unlock();
    return;
  }
  curr_size_--;
  node_store_[frame_id].ClearHistory();
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
