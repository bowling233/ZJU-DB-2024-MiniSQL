#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};//服务器做checkpoint结束时对应的LSN值，系统奔溃恢复时将从该值开始
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        active_txns_.insert(last_checkpoint.active_txns_.begin(),last_checkpoint.active_txns_.end());
        data_.insert(last_checkpoint.persist_data_.begin(),last_checkpoint.persist_data_.end());
    }

    /**
    * TODO: Student Implement
    */
    //从CheckPoint开始，根据不同日志的类型对KvDatabase和活跃事务列表作出修改
    void RedoPhase() {
        auto it = log_recs_.begin();
        //由于redo在checkpoint_lsn后边，恢复时可以不管它。
        while(1)
        {
            if(it != log_recs_.end() && it->first < persist_lsn_)
            {
                it++;
            }
            else
            break;
        }
        for(it;it != log_recs_.end();it++)
        {
            LogRec log=*(it->second);
            active_txns_[log.txn_id_] = log.lsn_;
            switch(log.type_)
            {
                case LogRecType::kInsert:   data_[log.key[1]] = log.val[1];break;
                case LogRecType::kDelete:   data_.erase(log.key[2]);break;
                case LogRecType::kUpdate:   data_.erase(log.key[0]);data_[log.key[3]]=log.val[3];break;
                case LogRecType::kCommit:   active_txns_.erase(log.txn_id_);break;
                case LogRecType::kAbort:    Rollback(log.txn_id_);active_txns_.erase(log.txn_id_);break;
                default:break;
            }
        }
    }
    void Rollback(txn_id_t txn_id) {
        lsn_t log_index=active_txns_[txn_id];
        while(1)
        {
            if(log_index == INVALID_LSN)
            break;
            LogRecPtr log = log_recs_[log_index];
            //no log
            if(log == nullptr)
            break;
            switch(log->type_)
            {
                case LogRecType::kInsert:   data_.erase(log->key[1]);break;
                case LogRecType::kDelete:   data_[log->key[2]] = log->val[2];break;
                case LogRecType::kUpdate:   data_.erase(log->key[3]);data_[log->key[0]]=log->val[0];break;
                default:break;
            }
            log_index=log->prev_lsn_;
        }
    }
    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        for(auto it:active_txns_)
        {
            Rollback(it.first);
        }
        active_txns_.clear();
    }

    

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
