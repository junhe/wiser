#include "engine_services.h"
#include "posting_list_protobuf.h"
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
/*
#define SIZE 100
    void *buffer[100];
    char **strings;

    int nptrs = backtrace(buffer, SIZE);
    printf("backtrace() returned %d addresses\n", nptrs);

   strings = backtrace_symbols(buffer, nptrs);
    if (strings == NULL) {
        perror("backtrace_symbols");
        exit(EXIT_FAILURE);
    }

   for (int j = 0; j < nptrs; j++)
        printf("%s\n", strings[j]);

   free(strings);

*/
/// debug

    _file_name_ = based_file;
    _last_offset_ = 0; 
    // open cache file
    _fd_ = open(based_file.c_str(), O_RDWR|O_CREAT|O_DIRECT,0600);
    //_fd_ = open(based_file.c_str(), O_RDWR|O_DIRECT);
    if ( _fd_ < 0 ) {
        std::string erro_str= "open file failed " + std::to_string(errno);
        throw std::runtime_error(erro_str);
    }
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
    //int len = segment.second;
    int len = PAGE_SIZE* ((segment.second + PAGE_SIZE -1 )/PAGE_SIZE);
    
    // read 
    // align buffer TODO better?
    char * buffer;
    int ret = posix_memalign((void **)&buffer, PAGE_SIZE, len);
    //std::cout << "align : " << ret << std::endl; 

    int size = pread(_fd_, buffer, len, start_offset);
    if (size < 0) {
        std::cout << "!!!!!!!!!!Alert: read failed" << std::endl; 
        throw std::runtime_error("Read failed");
    }
    std::string res(buffer, segment.second);
    free(buffer);
    //std::cout << size << " read in from " << start_offset << ", " << res.size() << std::endl;
    return res;
}

Store_Segment FlashReader::append(const std::string & value) {
    // append this value string to file
    char * buffer;
    int str_size = PAGE_SIZE* ((value.size() + PAGE_SIZE -1 )/PAGE_SIZE);
    int ret = posix_memalign((void **)&buffer, PAGE_SIZE, str_size);

    if (ret != 0)
        throw std::runtime_error("malloc using posix_memalign failed");
    
    std::copy(value.begin(), value.end(), buffer);
    //buffer[value.size()] = '\0';
        
    /* posting_message::Posting_Precomputed_4_Snippets p_message;
        
        // TODO?
        std::string str(buffer, value.size());
        //str.resize(value.size());
        p_message.ParseFromString(str);

        std::cout << "=======";
        std::cout << p_message.offsets_size() << ":" << p_message.passage_scores_size()  << ":" << p_message.passage_splits_size() << std::endl;  
        if (value.compare(str) != 0 ) {
            for (int i = 0; i< value.size(); i++)
                if (value[i]!=str[i]) {
                    std::cout << "Different at: " <<i << ": " << value[i] << "|" << str[i] << ";" << std::endl;
                }
            std::cout << "Doesnot equal: "  << value.compare(str)<< std::endl;
            std::cout << value.size() <<" : " << value << std::endl;
            std::cout << str.size() << " : " << str << std::endl;
            throw std::runtime_error("String does not equal");
        }
    */
    // write
    int size = pwrite(_fd_, buffer, str_size, _last_offset_);
    
    free(buffer); 
    if (size < 0) {
       std::cout << "!!!!!!!!!Alert: write failed, Errono: " << errno << std::endl; 
       throw std::runtime_error("write failed");
    }
    // update last_offset
    _last_offset_ += str_size;
    //return Store_Segment(_last_offset_-str_size, str_size);
    return Store_Segment(_last_offset_-str_size, value.size());
}
