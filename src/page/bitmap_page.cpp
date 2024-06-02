#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
/**
   * @param page_offset Index in extent of the page allocated.
   * @return true if successfully allocate a page.
   */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_>=GetMaxSupportedSize()){
    return false;//full
  }
  page_offset = next_free_page_;//allocate
  uint32_t byte_index = next_free_page_ / 8;
  uint8_t bit_index = next_free_page_ % 8;
  bytes[byte_index] = bytes[byte_index] | (1 << bit_index);

  //find next freepage(update)
  for(uint32_t i=0;i<GetMaxSupportedSize();i++){
    if(IsPageFreeLow(i/8,i%8)){
      next_free_page_ = i;
      break ;
    }
  }
  page_allocated_++;
  return true;
}

/**
 * TODO: Student Implement
 */
/**
   * @return true if successfully de-allocate a page.
   */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset>=GetMaxSupportedSize())
    return false;
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  //do not need to deallocate
  if((IsPageFreeLow(byte_index,bit_index))){
    return false;
  }
  bytes[byte_index]=bytes[byte_index] & (~(1 << bit_index));
  //update
  page_allocated_--;
  next_free_page_ = page_offset;
  return true;
}

/**
 * TODO: Student Implement
 */
/**
   * @return whether a page in the extent is free
   */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset>=GetMaxSupportedSize()){
    return false;//full
  }
  if(IsPageFreeLow(page_offset/8,page_offset%8)){
    return true;
  }else{
    return false;
  }
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
  return !((bytes[byte_index])&(1<<bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;