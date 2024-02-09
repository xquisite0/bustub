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
#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
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
  // std::cout << "Pool size " << pool_size << " with k " << replacer_k << std::endl;
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // frame_id_t *new_frame_ptr = nullptr;
    // std::cout << "Trying to evict a frame now\n";
    if (replacer_->Evict(&new_frame_id)) {
      // new_frame_id = *new_frame_ptr;
      // read latch for page?
      // std::cout << "We are now kicking out page " << pages_[new_frame_id].GetPageId() << " that is occupying frame "
      // << new_frame_id << "\n";
      pages_[new_frame_id].RLatch();
      page_table_.erase(pages_[new_frame_id].GetPageId());
      // std::cout << "Evicting page " << pages_[new_frame_id].GetPageId() << std::endl;
      pages_[new_frame_id].RUnlatch();
    } else {
      latch_.unlock();
      return nullptr;
    }
  }

  page_id_t new_page_id = AllocatePage();

  // check if replacement frame has a dirty page, and write it out if needed
  if (pages_[new_frame_id].IsDirty()) {
    DiskRequest r;
    r.is_write_ = true;

    pages_[new_frame_id].RLatch();
    r.data_ = pages_[new_frame_id].GetData();
    r.page_id_ = pages_[new_frame_id].GetPageId();
    pages_[new_frame_id].RUnlatch();

    std::future<bool> fut = r.callback_.get_future();
    disk_scheduler_->Schedule(std::move(r));

    fut.get();
    // may need to handle promises after this schedule, ignore for now
  }

  // resetting memory and metadata of frame
  pages_[new_frame_id].WLatch();
  pages_[new_frame_id].ResetMemory();
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].page_id_ = new_page_id;
  pages_[new_frame_id].is_dirty_ = false;
  pages_[new_frame_id].WUnlatch();

  // Pin the frame
  replacer_->SetEvictable(new_frame_id, false);
  replacer_->RecordAccess(new_frame_id);

  page_table_[new_page_id] = new_frame_id;

  *page_id = new_page_id;
  // std::cout << "in newpage, page " << new_page_id << " has frame " << new_frame_id << "\n";
  latch_.unlock();
  // std::cout << "Created page " << new_page_id << std::endl;
  return &pages_[new_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // std::cout << "Fetching page " << page_id << std::endl;
  // page is not in buffer pool
  // std::cout << "fetching page " << page_id << "\n";
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    // std::cout << "page " << page_id << " is not in the buffer pool\n";
    // pick replacement frame from 1. free list or 2. replacer
    frame_id_t new_frame_id;
    if (!free_list_.empty()) {
      new_frame_id = free_list_.back();
      free_list_.pop_back();
    } else {
      // frame_id_t *new_frame_ptr = nullptr;
      // std::cout << "trying to evict\n";
      if (replacer_->Evict(&new_frame_id)) {
        // new_frame_id = *new_frame_ptr;
        // read latch for page?
        // std::cout << "new_frame_id is " << new_frame_id << "\n";
        page_table_.erase(pages_[new_frame_id].GetPageId());
        // std::cout << "Evicting page " << pages_[new_frame_id].GetPageId() << std::endl;
      } else {
        latch_.unlock();
        return nullptr;
      }
    }

    // write out old page if dirty
    // std::cout << pages_[new_framex_id].IsDirty() << "\n";
    if (pages_[new_frame_id].IsDirty()) {
      DiskRequest r;
      r.is_write_ = true;

      pages_[new_frame_id].RLatch();
      r.data_ = pages_[new_frame_id].GetData();
      r.page_id_ = pages_[new_frame_id].GetPageId();
      pages_[new_frame_id].RUnlatch();
      std::future<bool> fut = r.callback_.get_future();
      disk_scheduler_->Schedule(std::move(r));

      fut.get();
      // may need to handle promises after this schedule, ignore for now
    }

    // resetting memory and metadata of frame
    pages_[new_frame_id].WLatch();
    pages_[new_frame_id].ResetMemory();
    pages_[new_frame_id].page_id_ = page_id;
    pages_[new_frame_id].pin_count_ = 1;
    pages_[new_frame_id].is_dirty_ = false;
    pages_[new_frame_id].WUnlatch();

    // Pin the frame
    replacer_->SetEvictable(new_frame_id, false);
    replacer_->RecordAccess(new_frame_id);

    page_table_[page_id] = new_frame_id;
    // std::cout << "memory of frame resetted\n";

    DiskRequest r;
    r.is_write_ = false;
    r.page_id_ = page_id;
    std::future<bool> fut = r.callback_.get_future();

    r.data_ = pages_[new_frame_id].data_;
    // char *disk_data = r.data_;
    disk_scheduler_->Schedule(std::move(r));
    // std::cout << "memory of frame resetted\n";

    // blocks until request completed
    fut.get();
    // if (fut.get()) {
    //   pages_[new_frame_id].data_ = disk_data;
    // }
    // delete[] disk_data;
    // std::cout << "memory of frame resetted\n";
  } else {
    pages_[page_table_[page_id]].WLatch();
    pages_[page_table_[page_id]].pin_count_++;
    pages_[page_table_[page_id]].WUnlatch();
    replacer_->SetEvictable(page_table_[page_id], false);
    replacer_->RecordAccess(page_table_[page_id]);
  }
  // std::cout << "in fetchpage, page " << page_id << "has frame " << page_table_[page_id] << "\n";
  latch_.unlock();
  return &pages_[page_table_[page_id]];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();

  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    // std::cout << "1ran! ";
    return false;
  }
  // std::cout << "Trying to unpin page " << page_id << std::endl;
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() == 0) {
    latch_.unlock();
    // std::cout << "2ran! ";
    return false;
  }

  pages_[frame_id].WLatch();
  pages_[frame_id].pin_count_--;
  // std::cout << "Page " << page_id << " is_dirty is " << is_dirty << std::endl;
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  pages_[frame_id].WUnlatch();

  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  // std::cout << "Flushing page " << page_id << std::endl;
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  // std::cout << "Flushing out page " << page_id << " now\n";
  DiskRequest r;
  r.is_write_ = true;
  r.data_ = pages_[page_table_[page_id]].GetData();
  r.page_id_ = page_id;
  std::future<bool> fut = r.callback_.get_future();
  disk_scheduler_->Schedule(std::move(r));

  // std::cout << "Awaiting DiskScheduler to write out page" << page_id << " to disk\n";
  fut.get();
  // std::cout << "Successfully written out page " << page_id << "\n";
  pages_[page_table_[page_id]].is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  // std::cout << "Flushing all pages\n";
  for (auto &p : page_table_) {
    FlushPage(p.first);
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  // std::cout << "Deleting page " << page_id << std::endl;
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  if (pages_[page_table_[page_id]].GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }

  replacer_->Remove(page_table_[page_id]);
  free_list_.push_back(page_table_[page_id]);
  pages_[page_table_[page_id]].ResetMemory();
  pages_[page_table_[page_id]].pin_count_ = 0;

  page_table_.erase(page_id);
  DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
