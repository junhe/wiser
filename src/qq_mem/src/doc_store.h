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
  header.Append(utils::MakeString(COMPRESSED_DOC_MAGIC));
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
  DLOG_IF(FATAL, (buf[0] & 0xFF) != COMPRESSED_DOC_MAGIC)
    << "Compressed doc has the wrong magic number: " << std::hex << buf[0];

  buf++; // skip the header
  VarintIteratorUnbounded it(buf); // we actually do not know the end, just use 
  *n_chunks = it.Pop();  
  for (std::size_t i = 0; i < *n_chunks; i++) {
    (*chunk_sizes)[i] = it.Pop();
  }

  return 1 + it.CurOffset(); // 1 is for the magic number
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
      LOG(FATAL) << "Failed to compresse: doc size = " << document.size();

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
  static const int buffer_size_ = 100*1024*1024;

	void AddRaw(const int id, const std::string document) {
    store_[id] = document;
  }
};


// Doc may be aligned
class ChunkedDocStoreDumper : public CompressedDocStore {
 public:
  ChunkedDocStoreDumper(bool align = true) 
    : CompressedDocStore(), align_(align) 
  {
    dump_buf_ = (char *)malloc(dump_buf_size_); 

    std::cout << "ChunkedDocStoreDumper::align_: " << align_ << std::endl;
  }

  // This will dump to two files, .fdx and .fdt
	void Dump(const std::string fdx_path, const std::string fdt_path) {
    std::ofstream fdtfile(fdt_path, std::ios::binary);
    std::ofstream fdxfile(fdx_path, std::ios::binary);

    int n_doc_ids = DumpHeader(fdxfile);

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
  int DumpHeader(std::ofstream &fdx) {
    int n_doc_ids;
    if (store_.size() == 0) {
      n_doc_ids = 0;
    } else {
      n_doc_ids = store_.rbegin()->first + 1;
    }
    VarintBuffer header;
    header.Append(n_doc_ids);
    header.Append(dump_buf_size_);

    fdx.write(header.Data().data(), header.Size());

    return n_doc_ids;
  }

  void DumpDoc(std::ofstream &fdt, std::ofstream &fdx, uint32_t doc_id) {
    if (align_) {
      DumpDocAligned(fdt, fdx, doc_id);
    } else {
      DumpDocUnaligned(fdt, fdx, doc_id);
    }
  }

  void DumpDocAligned(std::ofstream &fdt, std::ofstream &fdx, uint32_t doc_id) {
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

  void DumpDocUnaligned(std::ofstream &fdt, std::ofstream &fdx, uint32_t doc_id) {
    off_t offset = fdt.tellp();
    std::string chunk_compressed = CompressText(
        Get(doc_id), dump_buf_, dump_buf_size_);

    fdt.write(chunk_compressed.data(), chunk_compressed.size());

    off_t encoded_off = offset << 1;
    fdx.write(
        reinterpret_cast<const char *>(&encoded_off), sizeof(encoded_off));
  }

  const std::size_t dump_buf_size_ = 16 * KB;
  char *dump_buf_; 
  bool align_;
};


class ChunkedDocStoreReader {
 public:
  ChunkedDocStoreReader() {}

  void Load(const std::string fdx_path, const std::string fdt_path) {
    LoadFdx(fdx_path);
    MapFdt(fdt_path);
  }

  void LoadFdx(const std::string fdx_path) {
    std::cout << "Loading doc index..." << std::endl; 

    utils::FileMap file_map;
    file_map.Open(fdx_path);
    char *addr = file_map.Addr();

    VarintIteratorUnbounded iter(addr);
    n_doc_ids_ = iter.Pop();
    buffer_size_ = iter.Pop();
    InitializeBufferPool(buffer_size_);

    std::cout << "Loading fdx..." << std::endl;
    std::cout << "Number of doc ids: " << n_doc_ids_ << std::endl;
    std::cout << "Buffer size: " << buffer_size_ << std::endl;

    off_t offset = iter.CurOffset();
    for (int i = 0; i < n_doc_ids_; i++) {
      offset_store_.push_back(*(off_t *)(addr + offset));
      offset += sizeof(off_t);
    }

    std::cout << "Doc index loaded. Count:" << n_doc_ids_ << std::endl; 
    file_map.Close();
  }

  void MapFdt(const std::string fdt_path) {
    std::cout << "Mappign fdt..." << std::endl;
    fdt_map_.Open(fdt_path);

    std::cout << "Mappign fdt finished." << std::endl;
  }

  // You can only call this after MapFdt()
  void AdviseFdtRandom() {
    fdt_map_.MAdviseRand();
  }

  void UnmapFdt() {
    fdt_map_.Close();
  }

  bool Has(int id) {
    return id < n_doc_ids_;
  }

  const std::string Get(int id) { 
    off_t start_off = offset_store_[id];
    if ((start_off & 0x01) == 0x01) {  
      // was aligned
      start_off = start_off >> 1;
      start_off = start_off + 4096 - start_off % 4096;
    } else {
      start_off = start_off >> 1;
    }

    std::unique_ptr<char[]> buf = buffer_pool_.Get();

    std::string ret = DecompressText(
      fdt_map_.Addr() + start_off, buf.get(), buffer_size_);

    buffer_pool_.Put(std::move(buf));
    return ret;
  }

  int Size() const {
    return offset_store_.size() - 1; // minus the laste item (the guard)
  }

 private:
  void InitializeBufferPool(int buf_size) {
    buffer_pool_.Reset(8, buf_size);
  }

  int n_doc_ids_;
  BufferPool buffer_pool_;
  int buffer_size_;
  
  std::vector<long int> offset_store_;
  int fd_fdt_;
  utils::FileMap fdt_map_;
};








#endif
