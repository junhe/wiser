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
#include <lz4.h>

#include "engine_services.h"
#include "types.h"
#include "compression.h"
#include "simple_buffer_pool.h"


#define handle_error(msg) \
	do { perror(msg); exit(EXIT_FAILURE); } while (0)


#define DOCSTORE_FIRST_BYTE 0x32

namespace {

// text_len and buf size must be not larger than 16*1024
inline std::string CompressBoundedText(
    const char *text, std::size_t text_len, char *buf, std::size_t buf_size) {
  const int compressed_data_size = 
    LZ4_compress_default(text, buf, text_len, buf_size);

  if (compressed_data_size <= 0)
    LOG(FATAL) << "Failed to compresse";

  return std::string(buf, compressed_data_size);
}

inline std::string EncodeHeader(std::vector<std::size_t> chunk_sizes) {
  VarintBuffer header;
  header.Append(chunk_sizes.size()); // number of chunks
  for (auto &size : chunk_sizes) {
    header.Append(size);
  }

  return header.Data();
}

// 128*16*KB=2*MB
using chunk_sizes_t = std::array<std::size_t, 128>;

inline std::size_t DecodeHeader(
    std::size_t *n_chunks, chunk_sizes_t *chunk_sizes, const char *buf) 
{
  VarintIteratorUnbounded it(buf); // we actually do not know the end, just use 
  *n_chunks = it.Pop();  
  for (std::size_t i = 0; i < *n_chunks; i++) {
    (*chunk_sizes)[i] = it.Pop();
  }

  return it.CurOffset();
}

} // namespace


inline bool ShouldAlign(off_t start_off, std::size_t size) {
  int n_blocks_aligned = utils::DivideRoundUp(size, 4*KB);
  int n_blocks_unaligned = utils::DivideRoundUp((start_off % 4*KB) + size, 4*KB);

  return n_blocks_unaligned > n_blocks_aligned;
}


// Format:
// n_chunks:chunk_sz1:chunk_sz2:..:chunk_szN:chunk1:chunk2:...:chunkN
//
// buf_size should not be too small. It should be larger than 4KB
inline std::string CompressText(
    const std::string &text, char *buf, std::size_t buf_size) {
  std::size_t n_bytes_left = text.size();  
  const char *data = text.data();
  std::vector<std::size_t> chunk_sizes;
  std::string chunk_data;

  while (n_bytes_left > 0) {
    std::size_t text_len = std::min(n_bytes_left, buf_size / 2);
    std::string chunk = CompressBoundedText(data, text_len, buf, buf_size);
    chunk_sizes.push_back(chunk.size());
    chunk_data += chunk;

    data += text_len;
    n_bytes_left -= text_len;
  }

  return EncodeHeader(chunk_sizes) + chunk_data;
}

inline std::string DecompressText(
    const char *compressed, char *buf, std::size_t buf_size) {
  chunk_sizes_t chunk_sizes; 
  std::size_t n_chunks;
  std::string text;

  int chunk_start = DecodeHeader(&n_chunks, &chunk_sizes, compressed);
  const char *chunk = compressed + chunk_start;

  for (std::size_t i = 0; i < n_chunks; i++) {
    const int decompressed_size = 
      LZ4_decompress_safe(chunk, buf, chunk_sizes[i], buf_size);

    if (decompressed_size < 0) {
      LOG(FATAL) << "Failed to decompresse."; 
    }

    chunk += chunk_sizes[i];
    text += std::string(buf, decompressed_size);
  }

  return text;
}



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

    len = utils::varint_decode_uint32(data, offset, &var);
    offset += len;
    doc_id = var;

    len = utils::varint_decode_uint32(data, offset, &var);
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

    len = utils::varint_decode_uint32(addr, 0, &var);
    offset += len;
    int count = var;
    
    for (int i = 0; i < count; i++) {
      len = DeserializeEntry(addr + offset);
      offset += len;
    }

    utils::UnmapFile(addr, fd, file_length);
  }
};


class DocStoreBase {
 public:
	void Remove(int id) {
		store_.erase(id);
	}

	bool Has(int id) {
		return store_.count(id) == 1;
	}

	void Clear() {
		store_.clear();
	}

	int Size() const {
		return store_.size();
	}

	virtual void Add(int id, const std::string document) = 0;
	virtual const std::string Get(int id) = 0;

 protected:
	std::map<int, std::string> store_;  
};


class CompressedDocStore :public DocStoreBase {
 public:
	CompressedDocStore() {
    buffer_ = (char *) malloc(buffer_size_);
  }

  ~CompressedDocStore() {
    free(buffer_);
  }

	void Add(int id, const std::string document) override {
    const int compressed_data_size = 
      LZ4_compress_default(document.c_str(), buffer_, document.size(), buffer_size_);

    if (compressed_data_size <= 0)
      LOG(FATAL) << "Failed to compresse: '" << document << "'";

    store_[id] = std::string(buffer_, compressed_data_size);
	}

  // WARNING: not thread-safe
	const std::string Get(int id) override {
		auto &compressed_str = store_[id];
		const char *compressed_data = compressed_str.c_str();
    const int decompressed_size = 
      LZ4_decompress_safe(compressed_data, buffer_, compressed_str.size(), buffer_size_);
      
    if (decompressed_size < 0) {
      LOG(FATAL) << "Failed to decompresse."; 
    }
    
    return std::string(buffer_, decompressed_size);
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

    len = utils::varint_decode_uint32(data, offset, &var);
    offset += len;
    doc_id = var;

    len = utils::varint_decode_uint32(data, offset, &var);
    offset += len;
    text_size = var;
    
    std::string text(data + offset, text_size);
    
    AddRaw(doc_id, text); // Use this when the file is compressed
    // Add(doc_id, text); // Use this when the file is not compressed

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

    len = utils::varint_decode_uint32(addr, 0, &var);
    offset += len;
    int count = var;
    
    for (int i = 0; i < count; i++) {
      len = DeserializeEntry(addr + offset);
      offset += len;
    }

    utils::UnmapFile(addr, fd, file_length);
  }

 protected:
  char *buffer_;
  static const int buffer_size_ = 1024*1024;

	void AddRaw(const int id, const std::string document) {
    store_[id] = document;
  }
};


class FlashDocStoreDumper : public CompressedDocStore {
 public:
  FlashDocStoreDumper() : CompressedDocStore() {}

  // This will dump to two files, .fdx and .fdt
	void Dump(const std::string fdx_path, const std::string fdt_path) const {
    std::ofstream fdtfile(fdt_path, std::ios::binary);
    std::ofstream fdxfile(fdx_path, std::ios::binary);
    
    //fdx: max_docid: offset0, offset1, ... offset_docid  (if docid doesn't exist, -1)
    long int max_docid = (long int) store_.rbegin()->first;
    fdxfile.write(reinterpret_cast<const char *>(&max_docid), sizeof(max_docid));

    for (int docid = 0; docid <= max_docid; docid++) {
       // write out to fdt
       if (store_.count(docid) == 0) {
         LOG(FATAL) << "We do not allow holes in doc id";
       }

       long int offset = fdtfile.tellp();
       fdtfile.write(store_.at(docid).c_str(), store_.at(docid).length());

       // write out to fdx
       fdxfile.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
    }

    fdxfile.close();
    fdtfile.close();   
  }
};


class ChunkedDocStoreDumper : public CompressedDocStore {
 public:
  ChunkedDocStoreDumper() : CompressedDocStore() {
    dump_buf_ = (char *)malloc(dump_buf_size_); 
  }

  // This will dump to two files, .fdx and .fdt
	void Dump(const std::string fdx_path, const std::string fdt_path) {
    std::ofstream fdtfile(fdt_path, std::ios::binary);
    std::ofstream fdxfile(fdx_path, std::ios::binary);

    //fdx: max_docid: offset0, offset1, ... offset_docid  (if docid doesn't exist, -1)
    int n_doc_ids;
    if (store_.size() == 0) {
      n_doc_ids = 0;
    } else {
      n_doc_ids = store_.rbegin()->first + 1;
    }
    fdxfile.write(reinterpret_cast<const char *>(&n_doc_ids), sizeof(n_doc_ids));

    for (int docid = 0; docid < n_doc_ids; docid++) {
      if (store_.count(docid) == 0) {
        LOG(FATAL) << "We do not allow holes in doc id";
      }
      DumpDoc(fdtfile, fdxfile, docid);
    }

    fdxfile.close();
    fdtfile.close();   
  }

 private:
  void DumpDoc(std::ofstream &fdt, std::ofstream &fdx, uint32_t doc_id) {
    off_t offset = fdt.tellp();
    std::string chunk_compressed = CompressText(
        Get(doc_id), dump_buf_, dump_buf_size_);

    bool do_align = ShouldAlign(offset, chunk_compressed.size());
    if (do_align) {
      fdt.seekp((offset / (4 * KB) + 1) * 4*KB);
    }

    fdt.write(chunk_compressed.data(), chunk_compressed.size());

    off_t encoded_off = (offset << 1) | do_align;
    fdx.write(
        reinterpret_cast<const char *>(&encoded_off), sizeof(encoded_off));
  }

  const std::size_t dump_buf_size_ = 16 * KB;
  char *dump_buf_; 
};


// Either call:
//   Load()
// Or
//   LoadFdx()
//   MapFdt()
class FlashDocStore {
 public:
  FlashDocStore() :buffer_pool_(32, buffer_size_) {}

  void Load(const std::string fdx_path, const std::string fdt_path) {
    LoadFdx(fdx_path, fdt_path);
    MapFdt(fdt_path);
  }

  void LoadFdx(const std::string fdx_path, const std::string fdt_path) {
    std::cout << "Loading doc index..." << std::endl; 
    // open fdx, load index
    utils::FileMap file_map;
    file_map.Open(fdx_path);
    char *addr = file_map.Addr();

    off_t offset = 0;
    max_docid_ = *(long int *)addr;
    offset += sizeof(max_docid_);
    
    for (int i = 0; i <= max_docid_; i++) {
      offset_store_.push_back(*(long int *)(addr+offset));
      offset += sizeof(long int);
    }

    std::cout << "Doc index loaded." << std::endl; 
    file_map.Close();

    fdt_map_.Open(fdt_path);
    offset_store_.push_back(fdt_map_.Length());  // end of file
    fdt_map_.Close();
  }

  void MapFdt(const std::string fdt_path) {
    fdt_map_.Open(fdt_path);
  }

  ~FlashDocStore() {
    fdt_map_.Close();
  }

  bool Has(int id) {
    return id <= max_docid_;
  }

  const std::string Get(int id) {  
    long int start_off = offset_store_[id];
    long int doc_len = offset_store_[id + 1] - start_off;

    std::unique_ptr<char[]> buf = buffer_pool_.Get();

    // decompress
    const int decompressed_size = 
      LZ4_decompress_safe(fdt_map_.Addr() + start_off, buf.get(), doc_len, buffer_size_); 
    if (decompressed_size < 0) {
      LOG(FATAL) << "Failed to decompresse."; 
    }

    std::string ret = std::string(buf.get(), decompressed_size);
    buffer_pool_.Put(std::move(buf));

    return ret;
  }

  int Size() const {
    return offset_store_.size() - 1; // minus the laste item (the guard)
  }

  bool IsAligned() const {
    return true;
  }

 private:
  BufferPool buffer_pool_;
  static constexpr int buffer_size_ = 256 * 1024;
  
  std::vector<long int> offset_store_;
  long int max_docid_;
  int fd_fdt_;
  utils::FileMap fdt_map_;
};


class AlignedFlashDocStore {
 public:
  AlignedFlashDocStore() :buffer_pool_(32, buffer_size_) {}

  void Load(const std::string fdx_path, const std::string fdt_path) {
    LoadFdx(fdx_path, fdt_path);
    MapFdt(fdt_path);
  }

  void LoadFdx(const std::string fdx_path, const std::string fdt_path) {
    std::cout << "Loading doc index..." << std::endl; 
    // open fdx, load index
    utils::FileMap file_map;
    file_map.Open(fdx_path);
    char *addr = file_map.Addr();

    off_t offset = 0;
    max_docid_ = *(long int *)addr;
    offset += sizeof(max_docid_);
    
    for (int i = 0; i <= max_docid_; i++) {
      offset_store_.push_back(*(long int *)(addr+offset));
      offset += sizeof(long int);
    }

    std::cout << "Doc index loaded." << std::endl; 
    file_map.Close();

    fdt_map_.Open(fdt_path);
    offset_store_.push_back(fdt_map_.Length()*2);  // end of file
    fdt_map_.Close();
  }

  void MapFdt(const std::string fdt_path) {
    fdt_map_.Open(fdt_path);
  }

  ~AlignedFlashDocStore() {
    fdt_map_.Close();
  }

  bool Has(int id) {
    return id <= max_docid_;
  }

  const std::string Get(int id) { 
    // TODO different startoffset
    long int start_off = offset_store_[id]/2;
    if (offset_store_[id] % 2 == 1) {  // was aligned
      start_off = start_off + 4096 - start_off % 4096;
    }
    long int doc_len = offset_store_[id + 1] / 2 - start_off;

    std::unique_ptr<char[]> buf = buffer_pool_.Get();

    // decompress
    const int decompressed_size = 
      LZ4_decompress_safe(
          fdt_map_.Addr() + start_off, buf.get(), doc_len, buffer_size_); 
    if (decompressed_size < 0) {
      LOG(FATAL) << "Failed to decompresse." 
        << "doc len:" << doc_len; 
    }

    std::string ret = std::string(buf.get(), decompressed_size);
    buffer_pool_.Put(std::move(buf));

    return ret;
  }

  int Size() const {
    return offset_store_.size() - 1; // minus the laste item (the guard)
  }

  bool IsAligned() const {
    return true;
  }

 private:
  long int max_docid_;
  BufferPool buffer_pool_;
  static constexpr int buffer_size_ = 512 * 1024;
  
  std::vector<long int> offset_store_;
  int fd_fdt_;
  utils::FileMap fdt_map_;
};






#endif
