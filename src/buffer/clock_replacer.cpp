#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {}

CLOCKReplacer::~CLOCKReplacer() {}

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    if (clock_list.empty()) {
        return false;
    }
    while (true) {
        frame_id_t frame = clock_list.front();
        clock_list.pop_front();
        if (clock_status[frame] == 0) {
            *frame_id = frame;
            return true;
        } else {
            clock_status[frame] = 0;
            clock_list.push_back(frame);
        }
    }
    return false;
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
    clock_list.remove(frame_id);
    clock_status[frame_id] = 1;
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    clock_status[frame_id] = 0;
    clock_list.push_back(frame_id);
}

size_t CLOCKReplacer::Size() {
    return clock_list.size();
}