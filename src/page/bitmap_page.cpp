#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * @param page_offset Index in extent of the page allocated.
 * @return true if successfully allocate a page.
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // DLOG(INFO) << "Page Status: " << "page_allocated_ = " << page_allocated_ << ", next_free_page_ = " << next_free_page_ << std::endl;
  // Check if free page left
  if (next_free_page_ >= GetMaxSupportedSize()) {
    // LOG(INFO) << "No free page left" << std::endl;
    return false;
  }
  uint32_t byte_index = next_free_page_ / 8;
  uint8_t bit_index = next_free_page_ % 8;
  // Allocate next free page
  bytes[byte_index] |= (1 << bit_index);
  page_offset = next_free_page_;
  // DLOG(INFO) << "Allocate page " << page_offset << std::endl;
  // Find next free page
  page_allocated_++;
  if (page_allocated_ == GetMaxSupportedSize()) {
    next_free_page_ = GetMaxSupportedSize();
    return true;
  }
  do {
    next_free_page_ = (next_free_page_ + 1) % GetMaxSupportedSize();
    if (IsPageFree(next_free_page_)) {
      return true;
    }
    // DLOG(INFO) << "Next free page not free: " << next_free_page_ << std::endl;
  } while (next_free_page_ != page_offset);
  // DEBUG: This should not happen
  // DLOG(ERROR) << "Error: No free page left" << std::endl;
  throw std::exception();
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset >= GetMaxSupportedSize()) {
    LOG(ERROR) << "Page offset is out of range" << std::endl;
    throw std::out_of_range("Page offset is out of range");
  }
  // Check if page is already free
  if (IsPageFree(page_offset)) {
    // LOG(INFO) << "Page is already free" << std::endl;
    return false;
  }
  // De-allocate page
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  bytes[byte_index] &= ~(1 << bit_index);
  // Update next_free_page_
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }
  page_allocated_--;
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset >= GetMaxSupportedSize()) {
    LOG(ERROR) << "Page offset is out of range" << std::endl;
    throw std::out_of_range("Page offset is out of range");
  }
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

/**
 * check a bit(byte_index, bit_index) in bytes is free(value 0).
 *
 * @param byte_index value of page_offset / 8
 * @param bit_index value of page_offset % 8
 * @return true if a bit is 0, false if 1.
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return !((bytes[byte_index]) & (1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;