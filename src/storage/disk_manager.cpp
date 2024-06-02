#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  // DLOG(INFO) << "Meta page: " << meta_page->num_allocated_pages_ << " " << meta_page->num_extents_ << std::endl;
  if (meta_page->num_allocated_pages_ >= MAX_VALID_PAGE_ID && meta_page->num_extents_ >= MAX_EXTENT_NUMS) {
    LOG(ERROR) << "No enough space for new page." << std::endl;
    return INVALID_PAGE_ID;
  }
  // Find free page among existing extents
  // May be improved
  for (uint32_t extent = 0; extent < meta_page->GetExtentNums(); extent++) {
    //DLOG(INFO) << "Extent " << extent << " used pages: " << meta_page->extent_used_page_[extent] << std::endl;
    if (meta_page->extent_used_page_[extent] >= BITMAP_SIZE) {
      continue;
    }
    // Read the bitmap page
    page_id_t bitmap_page_id = 1 + extent * (BITMAP_SIZE + 1);
    BitmapPage<PAGE_SIZE> bitmap_page;
    ReadPhysicalPage(bitmap_page_id, reinterpret_cast<char *>(&bitmap_page));
    // Find and allocate a free page
    uint32_t page_id_in_extent;
    if (!bitmap_page.AllocatePage(page_id_in_extent)) {
      LOG(ERROR) << "Failed to allocate page in extent " << extent << std::endl;
      throw std::exception();
    }
    // Write the updated bitmap page
    WritePhysicalPage(bitmap_page_id, reinterpret_cast<char *>(&bitmap_page));
    page_id_t logical_page_id = extent * BITMAP_SIZE + page_id_in_extent;
    // (buffer will always set zero when creating page) Set the page to zero
    // char page_data[PAGE_SIZE];
    // memset(page_data, 0, PAGE_SIZE);
    // WritePage(logical_page_id, page_data);
    // Update meta page
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[extent]++;
    return logical_page_id;
  }
  // DLOG(INFO) << "Create new extent" << std::endl;
  // Not found in existing extents, create a new extent
  uint32_t new_extent = meta_page->GetExtentNums();
  // Create bitmap page for new extent
  BitmapPage<PAGE_SIZE> bitmap_page;
  uint32_t page_id_in_extent;
  if (!bitmap_page.AllocatePage(page_id_in_extent)) {
    LOG(ERROR) << "Failed to allocate page in new extent " << new_extent << std::endl;
    throw std::exception();
  }
  // Write the new bitmap page
  uint32_t bitmap_page_id = 1 + new_extent * (BITMAP_SIZE + 1);
  WritePhysicalPage(bitmap_page_id, reinterpret_cast<char *>(&bitmap_page));
  page_id_t logical_page_id = new_extent * BITMAP_SIZE + page_id_in_extent;
  // (buffer will always set zero when creating page) Set the page to zero
  // char page_data[PAGE_SIZE];
  // memset(page_data, 0, PAGE_SIZE);
  // WritePage(logical_page_id, page_data);
  // Update meta page
  meta_page->extent_used_page_[new_extent] = 1;
  meta_page->num_extents_++;
  meta_page->num_allocated_pages_++;
  return logical_page_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  if (logical_page_id >= MAX_VALID_PAGE_ID) {
    LOG(ERROR) << "Invalid page id: " << logical_page_id << std::endl;
    throw std::out_of_range("Invalid page id");
  }
  // Read the bitmap page
  page_id_t bitmap_page_id = 1 + (logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1); // = 1 + extent_num * (BITMAP_SIZE + 1)
  BitmapPage<PAGE_SIZE> bitmap_page;
  ReadPhysicalPage(bitmap_page_id, reinterpret_cast<char *>(&bitmap_page));
  // Deallocate the page
  uint32_t page_id_in_extent = logical_page_id % BITMAP_SIZE;
  if (!bitmap_page.DeAllocatePage(page_id_in_extent)) {
    LOG(ERROR) << "Failed to deallocate page " << logical_page_id << std::endl;
    throw std::exception();
  }
  // Write the updated bitmap page
  WritePhysicalPage(bitmap_page_id, reinterpret_cast<char *>(&bitmap_page));
  // Update meta page
  meta_page->num_allocated_pages_--;
  meta_page->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  if (logical_page_id >= MAX_VALID_PAGE_ID) {
    LOG(ERROR) << "Invalid page id: " << logical_page_id << std::endl;
    throw std::out_of_range("Invalid page id");
  }
  // Read the bitmap page
  page_id_t bitmap_page_id = 1 + (logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1);
  BitmapPage<PAGE_SIZE> bitmap_page;
  ReadPhysicalPage(bitmap_page_id, reinterpret_cast<char *>(&bitmap_page));
  // Check if the page is free
  uint32_t page_id_in_extent = logical_page_id % BITMAP_SIZE;
  bool is_free = bitmap_page.IsPageFree(page_id_in_extent);
  return is_free;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  if (logical_page_id >= MAX_VALID_PAGE_ID) {
    LOG(ERROR) << "Invalid page id: " << logical_page_id << std::endl;
    throw std::out_of_range("Invalid page id");
  }
  page_id_t extent = logical_page_id / BITMAP_SIZE;
  return 1 + logical_page_id + (1 + extent);
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
    // LOG(INFO) << "Read less than a page, physical page id:" << physical_page_id << std::endl;
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
      LOG(INFO) << "Read less than a page" << std::endl;
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}