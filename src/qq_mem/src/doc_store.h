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

#include "engine_services.h"
#include "types.h"
#include "compression.h"


#define handle_error(msg) \
	do { perror(msg); exit(EXIT_FAILURE); } while (0)


class SimpleDocStore: public DocumentStoreService {
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

	int Size() {
		return store_.size();
	}

  static std::string SerializeEntry(const int doc_id, const std::string &doc_text) {
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

  std::string SerializeCount() {
    VarintBuffer buf;
    buf.Append(store_.size());
    return buf.Data(); 
  }

  void Serialize(const std::string path) {
    std::ofstream ofile(path, std::ios::binary);

    std::string count_buf = SerializeCount();
    ofile.write(count_buf.data(), count_buf.size());

    for (auto it = store_.cbegin(); it != store_.cend(); it++) {
      std::string buf = SerializeEntry(it->first, it->second);
      ofile.write(buf.data(), buf.size());
    }

    ofile.close();	
  }

  void MapFile(std::string path, char **ret_addr, int *ret_fd, size_t *ret_file_length) {
    int fd;
    char *addr;
    size_t file_length;
    struct stat sb;

    fd = open(path.c_str(), O_RDONLY);

    if (fstat(fd, &sb) == -1)           /* To obtain file size */
      handle_error("fstat");
    file_length = sb.st_size;

    addr = (char *) mmap(NULL, file_length, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED)
      handle_error("mmap");

    *ret_addr = addr;
    *ret_fd = fd;
    *ret_file_length = file_length;
  }

  void UnmapFile(char *addr, int fd, size_t file_length) {
    munmap(addr, file_length);
    close(fd);
  }

  void Deserialize(const std::string path) {
    int fd;
    int len;
    char *addr;
    size_t file_length;
    uint32_t var;
    int offset = 0;

    MapFile(path, &addr, &fd, &file_length);

    len = utils::varint_decode_chars(addr, 0, &var);
    offset += len;
    int count = var;
    
    std::cout << "Count: " << count << std::endl;

    for (int i = 0; i < count; i++) {
      len = DeserializeEntry(addr + offset);
      offset += len;
    }

    UnmapFile(addr, fd, file_length);
  }
};

#endif
