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

#endif
