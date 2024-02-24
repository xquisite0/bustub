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
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // use move, because the copy constructor of promise is removed (callback_ of DiskRequest class)
  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    std::optional<DiskRequest> cur_request = request_queue_.Get();
    if (cur_request == std::nullopt) {
      break;
    }
    if (cur_request->is_write_) {
      // request writes into disk
      disk_manager_->WritePage(cur_request->page_id_, cur_request->data_);
    } else {
      // request reads from disk
      // std::cout << "trying to read page\n";
      // std::cout << cur_request->page_id_;
      disk_manager_->ReadPage(cur_request->page_id_, cur_request->data_);
      // std::cout << "successfully read page\n";
    }
    cur_request->callback_.set_value(true);
    // std::cout << "Successfully completed request with page_id " << cur_request->page_id_ << "\n";
  }
}

}  // namespace bustub
