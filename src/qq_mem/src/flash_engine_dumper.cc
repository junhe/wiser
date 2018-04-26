#include "flash_engine_dumper.h"

size_t SkipList::byte_cnt_ = 0;
std::mutex SkipList::mutex_;
std::set<const uint8_t *> SkipList::visited_;
