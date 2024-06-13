#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <random>
#include <string>

#include "gtest/gtest.h"

TEST(BufferPoolManagerTest, WriteDataTest) {
  const std::string db_name = "bpm_test.db";
  const size_t buffer_pool_size = 10;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<unsigned> uniform_dist(0, 127);

  remove(db_name.c_str());
  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));

  // Unpin and write to disk
  EXPECT_TRUE(bpm->UnpinPage(0, true));
  EXPECT_TRUE(bpm->FlushPage(0));

  // Then Fetch the page again

  auto *page1 = bpm->FetchPage(0);
  EXPECT_NE(nullptr, page1);
  // Compare the content
  for (int i = 0; i < PAGE_SIZE; i++) {
    EXPECT_EQ(random_binary_data[i], page1->GetData()[i]);
  }

  // Edit the page content
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }
  std::memcpy(page1->GetData(), random_binary_data, PAGE_SIZE);

  // Unpin and write to disk
  EXPECT_TRUE(bpm->UnpinPage(0, true));
  EXPECT_TRUE(bpm->FlushPage(0));

  // Fetch the page again
  auto *page2 = bpm->FetchPage(0);
  EXPECT_NE(nullptr, page2);
  // Compare the content
  for (int i = 0; i < PAGE_SIZE; i++) {
    EXPECT_EQ(random_binary_data[i], page2->GetData()[i]);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->Close();
  remove(db_name.c_str());

  delete bpm;
  delete disk_manager;
}