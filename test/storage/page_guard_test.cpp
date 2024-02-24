//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);
  // std::cout << "THIS RAN1\n\n\n";
  page_id_t page_id_temp;
  BasicPageGuard page0 = bpm->NewPageGuarded(&page_id_temp);

  Page *page0copy = bpm->FetchPage(0);
  page0.Drop();
  page0.Drop();
  EXPECT_EQ(page0copy->GetPinCount(), 1);

  BasicPageGuard page1 = bpm->NewPageGuarded(&page_id_temp);

  std::cout << "\n\nFetching page 1 now\n\n";
  Page *page1copy = bpm->FetchPage(1);
  // page1.Drop();
  std::cout << "\n\nUpgrading page 1 now\n\n";
  ReadPageGuard readpage1 = page1.UpgradeRead();
  EXPECT_EQ(2, page1copy->GetPinCount());
  // auto guarded_page = BasicPageGuard(bpm.get(), page0);
  
  // auto *page0copy = bpm->FetchPage(0);
  // auto guarded_pagecopy = BasicPageGuard(bpm.get(), page0copy);
  // EXPECT_EQ(page0copy->GetPinCount(), 1);
  //   // std::cout << "THIS RAN\n\n";

  // EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  // // std::cout << "THIS RAN1\n\n";
  // EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  // // std::cout << "THIS RAN2\n\n";
  // EXPECT_EQ(1, page0->GetPinCount());
  // auto * page1 = bpm->NewPage(&page_id_temp);
  // BasicPageGuard new_guarded_page = BasicPageGuard(bpm.get(), page1);
  // guarded_page = std::move(new_guarded_page);
  // EXPECT_EQ(0, page0->GetPinCount());
  // std::cout << "THIS RAN3\n\n";
  // bpm->UnpinPage(page_id_temp, false);
  // guarded_page.Drop();

  // EXPECT_EQ(0, page0->GetPinCount());

  {
    // std::cout << "THIS RAN4\n\n";
    auto *page2 = bpm->NewPage(&page_id_temp);
    auto guard2 = ReadPageGuard(bpm.get(), page2);
    // std::cout << "THIS RAN6\n\n";
  }
  // std::cout << "THIS RAN5\n\n";
  // Shutdown the disk manager and remove the temporary file we created.
  // BasicPageGuard page3 = bpm->NewPageGuarded(&page_id_temp);
  
  // BasicPageGuard page4 = bpm->NewPageGuarded(&page_id_temp);
  // page4 = std::move(page3);

  // EXPECT_EQ(0, page)

  disk_manager->ShutDown();


}

}  // namespace bustub
