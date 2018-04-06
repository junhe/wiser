#ifndef DOC_STORE_HH
#define DOC_STORE_HH

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <map>
#include <cereal/types/vector.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/archives/binary.hpp>
#include "lrucache.h"
#include <lz4.h>

#include "engine_services.h"
#include "types.h"
#include "compression.h"


#define handle_error(msg) \
	do { perror(msg); exit(EXIT_FAILURE); } while (0)

class SimpleDocStore {
 private:
	std::map<int,std::string> store_;  

 public:
	SimpleDocStore() {}

	void Add(int id, std::string document) {
		store_[id] = document;
	}

	void Remove(int id) {
		store_.erase(id);
	}

	bool Has(int id) {
		return store_.count(id) == 1;
	}

	const std::string &Get(int id) {
		return store_[id];
	}

	void Clear() {
		store_.clear();
	}

	int Size() const {
		return store_.size();
	}

  static std::string SerializeEntry(const int doc_id, 
      const std::string &doc_text) {
    VarintBuffer buf;
    buf.Append(doc_id);
    buf.Append(doc_text.size());
    buf.Append(doc_text);

    return buf.Data();
  }

  int DeserializeEntry(const char *data) {
    int len;
    uint32_t var;
    int doc_id, text_size;
    int offset = 0;

    len = utils::varint_decode_chars(data, offset, &var);
    offset += len;
    doc_id = var;

    len = utils::varint_decode_chars(data, offset, &var);
    offset += len;
    text_size = var;
    
    std::string text(data + offset, text_size);
    
    Add(doc_id, text);

    // return the length of this entry
    return offset + text_size;
  }

  std::string SerializeCount() const {
    VarintBuffer buf;
    buf.Append(store_.size());
    return buf.Data(); 
  }

  void Serialize(const std::string path) const {
    std::ofstream ofile(path, std::ios::binary);

    std::string count_buf = SerializeCount();
    ofile.write(count_buf.data(), count_buf.size());

    for (auto it = store_.cbegin(); it != store_.cend(); it++) {
      std::string buf = SerializeEntry(it->first, it->second);
      ofile.write(buf.data(), buf.size());
    }

    ofile.close();	
  }

  void Deserialize(const std::string path) {
    int fd;
    int len;
    char *addr;
    size_t file_length;
    uint32_t var;
    off_t offset = 0;

    utils::MapFile(path, &addr, &fd, &file_length);

    len = utils::varint_decode_chars(addr, 0, &var);
    offset += len;
    int count = var;
    
    for (int i = 0; i < count; i++) {
      len = DeserializeEntry(addr + offset);
      offset += len;
    }

    utils::UnmapFile(addr, fd, file_length);
  }
};


class CompressedDocStore {
 public:
	CompressedDocStore() {
    buffer_ = (char *) malloc(buffer_size_);
  }

  ~CompressedDocStore() {
    free(buffer_);
  }

	void Add(int id, const std::string document) {
    const int compressed_data_size = 
      LZ4_compress_default(document.c_str(), buffer_, document.size(), buffer_size_);

    if (compressed_data_size <= 0)
      LOG(FATAL) << "Failed to compresse: '" << document << "'";

    store_[id] = std::string(buffer_, compressed_data_size);
	}

	void Remove(int id) {
		store_.erase(id);
	}

	bool Has(int id) {
		return store_.count(id) == 1;
	}

	const std::string Get(int id) {
		auto &compressed_str = store_[id];
		const char *compressed_data = compressed_str.c_str();
    const int decompressed_size = 
      LZ4_decompress_safe(compressed_data, buffer_, compressed_str.size(), buffer_size_);
      
    if (decompressed_size < 0) {
      LOG(FATAL) << "Failed to decompresse."; 
    }
    
    return std::string(buffer_, decompressed_size);
	}

	void Clear() {
		store_.clear();
	}

	int Size() const {
		return store_.size();
	}

  static std::string SerializeEntry(const int doc_id, 
      const std::string &doc_text) {
    VarintBuffer buf;
    buf.Append(doc_id);
    buf.Append(doc_text.size());
    buf.Append(doc_text);

    return buf.Data();
  }

  int DeserializeEntry(const char *data) {
    int len;
    uint32_t var;
    int doc_id, text_size;
    int offset = 0;

    len = utils::varint_decode_chars(data, offset, &var);
    offset += len;
    doc_id = var;

    len = utils::varint_decode_chars(data, offset, &var);
    offset += len;
    text_size = var;
    
    std::string text(data + offset, text_size);
    
    // AddRaw(doc_id, text); // Use this when the file is compressed
    Add(doc_id, text); // Use this when the file is not compressed

    // return the length of this entry
    return offset + text_size;
  }

  std::string SerializeCount() const {
    VarintBuffer buf;
    buf.Append(store_.size());
    return buf.Data(); 
  }

  void Serialize(const std::string path) const {
    std::ofstream ofile(path, std::ios::binary);

    std::string count_buf = SerializeCount();
    ofile.write(count_buf.data(), count_buf.size());

    for (auto it = store_.cbegin(); it != store_.cend(); it++) {
      std::string buf = SerializeEntry(it->first, it->second);
      ofile.write(buf.data(), buf.size());
    }

    ofile.close();	
  }

  void Deserialize(const std::string path) {
    int fd;
    int len;
    char *addr;
    size_t file_length;
    uint32_t var;
    off_t offset = 0;

    utils::MapFile(path, &addr, &fd, &file_length);

    len = utils::varint_decode_chars(addr, 0, &var);
    offset += len;
    int count = var;
    
    for (int i = 0; i < count; i++) {
      len = DeserializeEntry(addr + offset);
      offset += len;
    }

    utils::UnmapFile(addr, fd, file_length);
  }

 private:
	std::map<int,std::string> store_;  
  char *buffer_;
  static const int buffer_size_ = 1024*1024;

	void AddRaw(const int id, const std::string document) {
    store_[id] = document;
  }
};

///////////////////////////////////////////////////////Flash Based////////////////////////////////////////////////////////////

class SimpleFlashCompressedDocStore {
 public:
  SimpleFlashCompressedDocStore(const std::string path) {
    std::cout << "=== initializing the doc store reader" << std::endl;
    buffer_ = (char *) malloc(buffer_size_); // TODO
    
    // open fdx, load index
    int fd;
    char *addr;
    size_t file_length;
    off_t offset = 0;
    utils::MapFile(path+"/fdx", &addr, &fd, &file_length);

    max_docid_ = *(long int *)addr;
    offset += sizeof(max_docid_);
    
    for (int i = 0; i <= max_docid_; i++) {
      offset_store_.push_back(*(long int *)(addr+offset));
      offset += sizeof(long int);
    }

    utils::UnmapFile(addr, fd, file_length);   // will it be in page cache?
    // open fdt, ready for query
    fd_fdt_ = open((path+"/fdt").c_str(), O_RDONLY);
    if (fd_fdt_ == -1) {
        std::cout << "Open file failed" << std::endl;
        exit(-1);
    }
    struct stat buf;
    fstat(fd_fdt_, &buf);
    offset_store_.push_back(buf.st_size);  // end of file
 
  }
  ~SimpleFlashCompressedDocStore() {
    free(buffer_);
  }

  void Dumpoffset() {
    std::cout << "Contents of offsets for documents: " << std::endl;
    for (int i = 0; i <= max_docid_+1; i++) {
      std::cout << i << ": " << offset_store_[i] << std::endl;
    }
  }
  
  bool Has(int id) {
    return (id <= max_docid_) && (offset_store_[id] != -1);
  }

  void Request(int id) {  // request a doc to be used in the future
    return;
  }

  const std::string Get(int id) {  // must get this document now
    long int start_off = offset_store_[id];
    long int doc_len = offset_store_[id+1] - start_off;
    std::cout << "+++ prepare to read from " << start_off << " len: " << doc_len << std::endl; 
    
    // read in from fdt
    char * compressed_data = (char *)malloc(doc_len);
    lseek(fd_fdt_, start_off, SEEK_SET);
    size_t size_read = read(fd_fdt_, compressed_data, doc_len);

    // decompress
    const int decompressed_size = 
      LZ4_decompress_safe(compressed_data, buffer_, doc_len, buffer_size_); 
    if (decompressed_size < 0) {
      LOG(FATAL) << "Failed to decompresse."; 
    }
    free(compressed_data);
    return std::string(buffer_, decompressed_size);
  }

  int Size() const {
    return max_docid_+1;
  }

 private:
  // TODO: this is insane
  char *buffer_;
  static const int buffer_size_ = 1024*1024;
  
  std::vector<long int> offset_store_;
  long int max_docid_;
  int fd_fdt_;
};


class SimpleFlashCompressedDocStoreIndex { // Class for index, require ids from 0...maxdocID all included
 public:
  SimpleFlashCompressedDocStoreIndex() {  // for index
    buffer_ = (char *) malloc(buffer_size_);
  }
  
  ~SimpleFlashCompressedDocStoreIndex() {
    free(buffer_);
  }
  
  void Clear() {
    store_.clear();
  }

  void Add(int id, const std::string document) {
    const int compressed_data_size = 
      LZ4_compress_default(document.c_str(), buffer_, document.size(), buffer_size_);
    std::cout << "Compressed from " << document.size() << " to: " << compressed_data_size << std::endl;
    if (compressed_data_size <= 0)
      LOG(FATAL) << "Failed to compresse: '" << document << "'";

    store_[id] = std::string(buffer_, compressed_data_size);
  }

  void Remove(int id) {
    store_.erase(id);
  }

  bool Has(int id) {
    return store_.count(id) == 1;
  }

  const std::string Get(int id) {
    auto &compressed_str = store_[id];
    const char *compressed_data = compressed_str.c_str();
    const int decompressed_size = 
      LZ4_decompress_safe(compressed_data, buffer_, compressed_str.size(), buffer_size_);
      
    if (decompressed_size < 0) {
      LOG(FATAL) << "Failed to decompresse."; 
    }
    return std::string(buffer_, decompressed_size);
  }

  int Size() const {
    return store_.size();
  }

  void Serialize(const std::string dir_path) const {
    std::cout << "=== To serialize: " << std::endl;
    std::ofstream fdtfile(dir_path+"/fdt", std::ios::binary);
    std::ofstream fdxfile(dir_path+"/fdx", std::ios::binary);
    
    //fdx: max_docid: offset0, offset1, ... offset_docid  (if docid doesn't exist, -1)
    long int max_docid = (long int) store_.rbegin()->first;
    fdxfile.write(reinterpret_cast<const char *>(&max_docid), sizeof(max_docid));

    for (int docid = 0; docid <= max_docid; docid++) {
       // write out to fdti
       long int offset = -1;
       if (store_.count(docid) == 1) {
           offset = fdtfile.tellp();
           fdtfile.write(store_.at(docid).c_str(), store_.at(docid).length());
       }
       std::cout << offset << std::endl;
       fdxfile.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
    }

    fdxfile.close();
    fdtfile.close();
  }

 private:
  std::map<int,std::string> store_;
  char *buffer_;
  static const int buffer_size_ = 1024*1024;

  void AddRaw(const int id, const std::string document) {
    store_[id] = document;
  }
};


#endif
