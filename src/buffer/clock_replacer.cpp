#include "buffer/clock_replacer.h"
#include <iostream>
#include "glog/logging.h"

/**
 * The size of the ClockReplacer is the same as buffer pool 
 * since it contains placeholders for all of the frames in the BufferPoolManager. 
 * However, not all the frames are considered as in the ClockReplacer. 
 * The ClockReplacer is initialized to have no frame in it. 
 * Then, only the newly unpinned ones will be considered in the ClockReplacer. 
 * Adding a frame to or removing a frame from a replacer is implemented by changing a reference bit of a frame. 
 * The clock hand initially points to the placeholder of frame 0. 
*/


/**
 * Create a new ClockReplacer.
 * @param num_pages the maximum number of pages the ClockReplacer will be required to store
 */
CLOCKReplacer::CLOCKReplacer(size_t num_pages)
{
    //initialized to have no frame in it
    capacity=num_pages;
    for (int i = 0; i < num_pages; ++i) {
    clock_list.emplace_back(INVALID_FRAME_ID);
    }
    clock_hand = clock_list.begin();
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    if (Size() == 0) {
        return false;
    }

    while(1)
    {
        if (*clock_hand != INVALID_FRAME_ID) {
            if (clock_status[*clock_hand]) {
                // 如果 ref flag 为 true，则将其置为 false 并继续下一个
                if (clock_status[*clock_hand] == 1)
                clock_status[*clock_hand] = 0;
            } else {//找到
                *frame_id = *clock_hand;
                clock_status.erase(*clock_hand);
                *clock_hand = INVALID_FRAME_ID;// 找到了之后，clock_hand_ 不需要再 +1，下次还是从这个地方开始扫描
                return true;
            }
        }
        clock_hand++;// 如果该 frame 不存在，指向下一个

        if (clock_hand == clock_list.end())// clock_hand 超过 frames 的长度，则置 0，模拟时钟的重新循环
            clock_hand = clock_list.begin();
    }

    *frame_id = INVALID_FRAME_ID;
    return false;
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
    if (clock_status.count(frame_id)) {
        clock_status[frame_id] = 2;
    }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    //找到，将其ref设为1
    if (clock_status.count(frame_id)) {
        clock_status[frame_id] = 1;
    }
    else {
        int flag=0;
        list<frame_id_t>::iterator i;
        i=clock_list.begin();
        for(i;i!=clock_list.end();i++)//find a place to insert it
        {
            if(*i==INVALID_FRAME_ID)//找到
            {
                *i = frame_id;
                clock_status[frame_id] = 1;
                break;
            }
        }
    }
}

size_t CLOCKReplacer::Size() {
  size_t size = 0;
  for (auto i : clock_list) {
    if(i!=INVALID_FRAME_ID&&clock_status[i]!=2)
        size++;
  }
  return size;
}