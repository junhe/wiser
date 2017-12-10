#include "engine_services.h"
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string.h>

FlashReader::FlashReader(const std::string & based_file) {
    _file_name_ = based_file;
    _last_offset_ = 0; 
    // open cache file
    _fd_ = open(based_file.c_str(), O_RDWR|O_CREAT|O_SYNC|O_DIRECT);
}

FlashReader::~FlashReader() {
    // close cache file
    // delete file content
    close(_fd_);
    _fd_ = open(_file_name_.c_str(), O_TRUNC);
    close(_fd_);
}
        
std::string FlashReader::read(const Store_Segment & segment) {
    // read from file, in area of segment
    int start_offset = segment.first;
    int len = segment.second;
    
    // read 
    // align buffer TODO better?
    char * buffer;
    int ret = posix_memalign((void **)&buffer, PAGE_SIZE, len);
    //std::cout << "align : " << ret << std::endl; 

    int size = pread(_fd_, buffer, len, start_offset);
    if (size < 0)
       std::cout << "!!!!!!!!!!Alert: read failed" << std::endl; 
    std::string res(buffer);
    free(buffer);
    res.resize(len);     //TODO from char* to string, size changed
    //std::cout << size << " read in " << res << " from " << start_offset << ", " << res.size() << std::endl;
    return res;
}

Store_Segment FlashReader::append(const std::string & value) {
    // append this value string to file
    char * buffer;
    int str_size = PAGE_SIZE* ((value.size()+1 + PAGE_SIZE -1 )/PAGE_SIZE);
    int ret = posix_memalign((void **)&buffer, PAGE_SIZE, str_size);


    //std::cout << "align : " << str_size <<" " << ret << std::endl; 
    memcpy(buffer, value.c_str(), value.size() + 1);

    // write 
    int size = pwrite(_fd_, buffer, str_size, _last_offset_);
    
    free(buffer); 
    if (size < 0)
       std::cout << "!!!!!!!!!Alert: write failed" << std::endl; 
    
    // update last_offset
    _last_offset_ += str_size;
    std::cout << size << " write out "  << " to " << _last_offset_-str_size << ", " << str_size << std::endl; 
    return Store_Segment(_last_offset_-str_size, str_size);
}
