#include "concurrency/lock_manager.h"

#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"
#include "gtest/gtest.h"

#include "glog/logging.h"

inline void CheckGrowing(Txn &txn) { ASSERT_EQ(TxnState::kGrowing, txn.GetState()); }

inline void CheckShrinking(Txn &txn) { ASSERT_EQ(TxnState::kShrinking, txn.GetState()); }

inline void CheckAborted(Txn &txn) { ASSERT_EQ(TxnState::kAborted, txn.GetState()); }

inline void CheckCommitted(Txn &txn) { ASSERT_EQ(TxnState::kCommitted, txn.GetState()); }

inline void CheckTxnLockSize(Txn &txn, size_t shared_expected, size_t exclusive_expected) {
  ASSERT_EQ(shared_expected, txn.GetSharedLockSet().size());
  ASSERT_EQ(exclusive_expected, txn.GetExclusiveLockSet().size());
}

class LockManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    lock_mgr_ = new LockManager();
    txn_mgr_ = new TxnManager(lock_mgr_);
  }

  void TearDown() override {
    delete txn_mgr_;
    delete lock_mgr_;
  }

 protected:
  LockManager *lock_mgr_{nullptr};
  TxnManager *txn_mgr_{nullptr};
};

// N 个事务在数据上创建共享锁，然后依次（并发）尝试升级，升级成功后 Commit
// 释放锁。应当只有第一个事务能够在所有后续事务尝试升级但因为冲突升级未能成功释放锁后成功升级并commit。
TEST_F(LockManagerTest, BulkUpdateTest) {
  // 准备死锁检测
  auto cycle_detection_interval = std::chrono::milliseconds(500);
  lock_mgr_->EnableCycleDetection(cycle_detection_interval);
  std::thread detect_worker(std::bind(&LockManager::RunCycleDetection, lock_mgr_));

  RowId row(0, 0);
  const uint32_t n = 1000;
  std::vector<Txn *> txn(n);
  for (uint32_t i = 0; i < n; i++) {
    txn[i] = txn_mgr_->Begin();
    lock_mgr_->LockShared(txn[i], row);
  }
  // 新建线程尝试升级
  std::thread threads[n];
  for (uint32_t i = 0; i < n; i++) {
    threads[i] = std::thread([this, i, &row, &txn] {
      //DLOG(INFO) << "Thread " << i << " try to upgrade";
      bool res;
      try {
        res = lock_mgr_->LockUpgrade(txn[i], row);
      } catch (TxnAbortException &e) {
        ASSERT_EQ(AbortReason::kUpgradeConflict, e.abort_reason_);
        ASSERT_EQ(TxnState::kAborted, txn[i]->GetState());
        txn_mgr_->Abort(txn[i]);
        //DLOG(INFO) << "Thread " << i << " aborted";
        return;
      }
      ASSERT_TRUE(res);
      txn_mgr_->Commit(txn[i]);
      ASSERT_EQ(TxnState::kCommitted, txn[i]->GetState());
      //DLOG(INFO) << "Thread " << i << " committed";
    });
  }
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  // 等待线程结束
  for (auto &t : threads) {
    t.join();
  }

  lock_mgr_->DisableCycleDetection();
  detect_worker.join();
  for (auto &t : txn) {
    delete t;
  }
}

// 大量二阶段锁事务并发请求。N 个事务在数据上随机创建共享锁或排他锁，创
// 建成功后等待一定时间 Commit 释放锁。应当所有事务都能够成功 Commit。

TEST_F(LockManagerTest, BulkTwoPhaseLockTest) {
  // 无需死锁检测

  RowId row(0, 0);
  const uint32_t n = 100;
  std::vector<Txn *> txn(n);
  std::thread threads[n];
  // 新建并发线程，每个线程是一个完整的事务
  for (uint32_t i = 0; i < n; i++) {
    txn[i] = txn_mgr_->Begin();
    //DLOG(INFO) << "Thread " << i << " begin";
    if (i % 2 == 0) {
      threads[i] = std::thread([this, i, &row, &txn] {
        lock_mgr_->LockShared(txn[i], row);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        txn_mgr_->Commit(txn[i]);
        ASSERT_EQ(TxnState::kCommitted, txn[i]->GetState());
        //DLOG(INFO) << "Thread " << i << " committed";
      });
    } else {
      threads[i] = std::thread([this, i, &row, &txn] {
        lock_mgr_->LockExclusive(txn[i], row);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        txn_mgr_->Commit(txn[i]);
        ASSERT_EQ(TxnState::kCommitted, txn[i]->GetState());
        //DLOG(INFO) << "Thread " << i << " committed";
      });
    }
  }

  // 等待线程结束
  for (auto &t : threads) {
    t.join();
  }

  for (auto &t : txn) {
    delete t;
  }
}
