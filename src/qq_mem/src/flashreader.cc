#include "engine_services.h"

FlashReader::FlashReader(const std::string & based_file) {
   _file_name_ = based_file;
   _last_offset_ = 0; 
   // TODO open file

}
        
std::string FlashReader::read(const Store_Segment & segment) {
    // read from file, in area of segment
    int start_offset = segment.first;
    int len = segment.second;
    // TODO read 
    return "";
}

Store_Segment FlashReader::append(const std::string & value) {
    // append this value string to file
    // update last_offset
    _last_offset_ += 10;
    // TODO store
    return Store_Segment(_last_offset_-10, 10);
}
