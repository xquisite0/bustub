#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  // std::cout << "Basic Page " << page_->GetPageId() << " has been move constructed\n";
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr) {
    return;
  }
  // std::cout << "Basic Page " << page_->GetPageId() << " was dropped\n";
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (bpm_ != nullptr) {
    Drop();
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  // std::cout << "Basic Page " << page_->GetPageId() << " has been move assigned\n";

  return *this;
}

BasicPageGuard::~BasicPageGuard(){};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  BufferPoolManager *cur_bpm = bpm_;
  Page *cur_page = page_;

  // need to check for nullptr?
  if (bpm_ != nullptr) {
    cur_page->RLatch();
  }
  bpm_ = nullptr;
  page_ = nullptr;
  // std::cout << "Basic Page " << cur_page->GetPageId() << " has been upgraded to a read\n";
  return {cur_bpm, cur_page};
};
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  BufferPoolManager *cur_bpm = bpm_;
  Page *cur_page = page_;
  if (bpm_ != nullptr) {
    cur_page->WLatch();
  }
  bpm_ = nullptr;
  page_ = nullptr;
  // std::cout << "Basic Page " << cur_page->GetPageId() << " has been upgraded to a write page\n";
  return {cur_bpm, cur_page};
};

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  // std::cout << "Read page " << guard_.PageId() << " has been move constructed\n";
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_ = std::move(that.guard_);
  // std::cout << "Read page " << guard_.PageId() << " has been move assigned\n";
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ == nullptr) {
    return;
  }
  // std::cout << "Read page " << guard_.PageId() << " has been dropped\n";
  // guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  // std::cout << "Write page " << guard_.PageId() << " has been move constructed";
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_ = std::move(that.guard_);
  // std::cout << "Write page " << guard_.PageId() << " has been move assigned\n";
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ == nullptr) {
    return;
  }
  // std::cout << "Write page " << guard_.PageId() << " has been dropped\n";
  // guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), true);
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
  }
}

}  // namespace bustub
