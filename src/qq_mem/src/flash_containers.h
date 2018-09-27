#ifndef FLASH_CONTAINERS_H
#define FLASH_CONTAINERS_H

#include "types.h"
#include "packed_value.h"

#include "tsl/htrie_hash.h"
#include "tsl/htrie_map.h"

constexpr int SKIP_INTERVAL = PACK_ITEM_CNT;
constexpr int PACK_SIZE = PACK_ITEM_CNT;


inline void DecodePrefetchZoneAndOffset(
    const off_t offset_from_term_index, uint32_t *n_pages_of_zone, off_t *posting_list_start) {
  *n_pages_of_zone = ((uint64_t) offset_from_term_index) >> 48;
  constexpr uint64_t mask = (((~(uint64_t) 0) << 16) >> 16); // 0b0000 111..111   48 1s
  *posting_list_start = ((uint64_t)offset_from_term_index) & mask;
}


inline std::vector<uint32_t> GetSkipPostingPreDocIds(const std::vector<uint32_t> &doc_ids) { std::vector<uint32_t> skip_pre_doc_ids{0}; // the first is always 0
  for (std::size_t skip_posting_i = SKIP_INTERVAL; 
      skip_posting_i < doc_ids.size(); 
      skip_posting_i += SKIP_INTERVAL) 
  {
    skip_pre_doc_ids.push_back(doc_ids[skip_posting_i - 1]); 
  }
  return skip_pre_doc_ids;
}


inline void DecodeBitmapByte(const uint8_t byte, bool *bitmap) {
  bitmap[0] = byte & 0x80;
  bitmap[1] = byte & 0x40;
  bitmap[2] = byte & 0x20;
  bitmap[3] = byte & 0x10;
  bitmap[4] = byte & 0x08;
  bitmap[5] = byte & 0x04;
  bitmap[6] = byte & 0x02;
  bitmap[7] = byte & 0x01;
}

struct PostingBagBlobIndex {
  PostingBagBlobIndex(int block_idx, int offset_idx)
    : blob_index(block_idx), in_blob_idx(offset_idx) {}

  int blob_index;
  int in_blob_idx;
};

struct SkipListPreRow {
  void Reset() {
    pre_doc_id = 0; 
    doc_id_blob_off = 0;
    tf_blob_off = 0;
    pos_blob_off = 0;
    off_blob_off = 0;
  }
  uint32_t pre_doc_id = 0; 
  off_t doc_id_blob_off = 0;
  off_t tf_blob_off = 0;
  off_t pos_blob_off = 0;
  off_t off_blob_off = 0;
};





class PostingBagBlobIndexes {
 public:
  void AddRow(int block_idx, int offset) {
    locations_.emplace_back(block_idx, offset);
  }

  int NumRows() const {
    return locations_.size();
  }

  const PostingBagBlobIndex & operator[] (int i) const {
    return locations_[i];
  }

 private:
  std::vector<PostingBagBlobIndex> locations_;
};


class FileOffsetsOfBlobs {
 public:
  FileOffsetsOfBlobs(std::vector<off_t> pack_offs, std::vector<off_t> vint_offs)
      : pack_offs_(pack_offs), vint_offs_(vint_offs) {
  }

  int PackOffSize() const {
    return pack_offs_.size();
  }

  int VIntsSize() const {
    return vint_offs_.size();
  }

  const std::vector<off_t> &PackOffs() const {
    return pack_offs_;
  }

  const std::vector<off_t> &VIntsOffs() const {
    return vint_offs_;
  }

  const std::vector<off_t> BlobOffsets() const {
    std::vector<off_t> offsets;
    for (auto &off : pack_offs_) {
      offsets.push_back(off);
    }

    for (auto &off : vint_offs_) {
      offsets.push_back(off);
    }

    return offsets;
  }

  off_t FileOffset(int blob_index) const {
    const int n_packs = pack_offs_.size();
    if (blob_index < n_packs) {
      return pack_offs_[blob_index];
    } else if (blob_index == n_packs) {
      if (vint_offs_.size() == 0)
        LOG(FATAL) << "vint_offs_.size() should not be 0.";
      return vint_offs_[0];
    } else {
      LOG(FATAL) << "blob_index is too large.";
      return -1; // suppress warning
    }
  }

  friend bool operator == (
      const FileOffsetsOfBlobs &a, const FileOffsetsOfBlobs &b) {
    if (a.pack_offs_.size() != b.pack_offs_.size())
      return false;

    for (std::size_t i = 0; i < a.pack_offs_.size(); i++) {
      if (a.pack_offs_[i] != b.pack_offs_[i]) {
        return false;
      }
    }

    if (a.vint_offs_.size() != b.vint_offs_.size())
      return false;

    for (std::size_t i = 0; i < a.vint_offs_.size(); i++) {
      if (a.vint_offs_[i] != b.vint_offs_[i]) {
        return false;
      }
    }
    return true;
  }

  friend bool operator != (
      const FileOffsetsOfBlobs &a, const FileOffsetsOfBlobs &b) {
    return !(a == b);
  }

 private:
  std::vector<off_t> pack_offs_;
  std::vector<off_t> vint_offs_;
};






class CozyBoxWriter {
 public:
  CozyBoxWriter(std::vector<LittlePackedIntsWriter> writers, 
                     VIntsWriter vints)
    :pack_writers_(writers), vints_(vints) {}

  const std::vector<LittlePackedIntsWriter> &PackWriters() const {
    return pack_writers_;
  }

  const VIntsWriter &VInts() const {
    return vints_;
  }

 private:
  std::vector<LittlePackedIntsWriter> pack_writers_;
  VIntsWriter vints_;
};


struct FileOffsetOfSkipPostingBag {
  FileOffsetOfSkipPostingBag(off_t offset, int index)
    : file_offset_of_blob(offset), in_blob_index(index) {}

  off_t file_offset_of_blob;
  int in_blob_index;
};


// Absolute file offsets for posting 0, 128, 128*2, ..'s data
class FileOffsetOfSkipPostingBags {
 public:
  FileOffsetOfSkipPostingBags(const PostingBagBlobIndexes &table, 
      const FileOffsetsOfBlobs &file_offs) {
    for (int posting_index = 0; 
        posting_index < table.NumRows(); 
        posting_index += SKIP_INTERVAL) 
    {
      const int pack_id = table[posting_index].blob_index;
      const int in_blob_idx = table[posting_index].in_blob_idx;
      if (file_offs.FileOffset(pack_id) < 0) {
        LOG(FATAL) << "File offset of pack " << pack_id << " is < 0";
      }
      locations_.emplace_back(file_offs.FileOffset(pack_id), in_blob_idx);
    }
  }

  std::size_t Size() const {
    return locations_.size();
  }

  const FileOffsetOfSkipPostingBag &operator [](int i) const {
    return locations_[i];
  }

 private:
  std::vector<FileOffsetOfSkipPostingBag> locations_;
};


class SkipListWriter {
 public:
  SkipListWriter(const FileOffsetOfSkipPostingBags docid_file_offs,
           const FileOffsetOfSkipPostingBags tf_file_offs,
           const FileOffsetOfSkipPostingBags pos_file_offs,
           const FileOffsetOfSkipPostingBags off_file_offs,
           const std::vector<uint32_t> doc_ids) 
    :docid_offs_(docid_file_offs),
     tf_offs_(tf_file_offs),
     pos_offs_(pos_file_offs),
     off_offs_(off_file_offs),
     doc_ids_(doc_ids)
  {}

  std::string Serialize() {
    pre_row_.Reset();
    VarintBuffer buf;
    auto skip_pre_doc_ids = GetSkipPostingPreDocIds(doc_ids_);

    if ( !(docid_offs_.Size() == tf_offs_.Size() && 
           tf_offs_.Size() == pos_offs_.Size() &&
           pos_offs_.Size() == off_offs_.Size() &&
           ( off_offs_.Size() == skip_pre_doc_ids.size() || 
             off_offs_.Size() + 1 == skip_pre_doc_ids.size())
           ) ) 
    {
      LOG(INFO)
        <<    docid_offs_.Size() << ", " 
        <<    tf_offs_.Size() << ", "
        <<    pos_offs_.Size() << ", "
        <<    off_offs_.Size() << ", "
        <<    skip_pre_doc_ids.size() << std::endl;
      LOG(FATAL) << "Skip data is not uniform";
    }

    int n_rows = docid_offs_.Size();
    buf.Append(utils::MakeString(SKIP_LIST_FIRST_BYTE));
    buf.Append(n_rows);
    for (int i = 0; i < n_rows; i++) {
      AddRow(&buf, i, skip_pre_doc_ids);
    }

    return buf.Data();
  }

 private:
  void AddRow(VarintBuffer *buf, int i, 
      const std::vector<uint32_t> skip_pre_doc_ids) {

    buf->Append(skip_pre_doc_ids[i] - pre_row_.pre_doc_id);
    buf->Append(docid_offs_[i].file_offset_of_blob - pre_row_.doc_id_blob_off);
    buf->Append(tf_offs_[i].file_offset_of_blob - pre_row_.tf_blob_off);
    buf->Append(pos_offs_[i].file_offset_of_blob - pre_row_.pos_blob_off);
    buf->Append(pos_offs_[i].in_blob_index);
    buf->Append(off_offs_[i].file_offset_of_blob - pre_row_.off_blob_off);
    buf->Append(off_offs_[i].in_blob_index);

    // update the prev
    pre_row_.pre_doc_id = skip_pre_doc_ids[i];
    pre_row_.doc_id_blob_off = docid_offs_[i].file_offset_of_blob ;
    pre_row_.tf_blob_off = tf_offs_[i].file_offset_of_blob;
    pre_row_.pos_blob_off = pos_offs_[i].file_offset_of_blob;
    pre_row_.off_blob_off = off_offs_[i].file_offset_of_blob;
  }

  SkipListPreRow pre_row_;

  const FileOffsetOfSkipPostingBags docid_offs_;
  const FileOffsetOfSkipPostingBags tf_offs_;
  const FileOffsetOfSkipPostingBags pos_offs_;
  const FileOffsetOfSkipPostingBags off_offs_;
  const std::vector<uint32_t> doc_ids_;
};


// All locations are for posting bags
struct SkipEntry {
  SkipEntry(const uint32_t doc_skip_in,   
            const off_t doc_file_offset_in,
            const off_t tf_file_offset_in,
            const off_t pos_file_offset_in,
            const int pos_in_block_index_in,
            const off_t off_file_offset_in,
            const int offset_in_block_index_in)
    : previous_doc_id(doc_skip_in),
      file_offset_of_docid_bag(doc_file_offset_in),
      file_offset_of_tf_bag(tf_file_offset_in),
      file_offset_of_pos_blob(pos_file_offset_in),
      in_blob_index_of_pos_bag(pos_in_block_index_in),
      file_offset_of_offset_blob(off_file_offset_in),
      in_blob_index_of_offset_bag(offset_in_block_index_in)
  {}
 
  uint32_t previous_doc_id;
  off_t file_offset_of_docid_bag;
  off_t file_offset_of_tf_bag;
  off_t file_offset_of_pos_blob;
  int in_blob_index_of_pos_bag;
  off_t file_offset_of_offset_blob;
  int in_blob_index_of_offset_bag;

  std::string ToStr() const {
    std::string ret;

    ret += std::to_string(previous_doc_id) + "\t";
    ret += std::to_string(file_offset_of_docid_bag) + "\t";
    ret += std::to_string(file_offset_of_tf_bag) + "\t";
    ret += std::to_string(file_offset_of_pos_blob) + "\t";
    ret += std::to_string(in_blob_index_of_pos_bag) + "\t";
    ret += std::to_string(file_offset_of_offset_blob) + "\t";
    ret += std::to_string(in_blob_index_of_offset_bag) + "\t";

    return ret;
  }
};

class SkipList {
 public:
  void Load(const uint8_t *buf) {
    // byte 0 is the magic number
    DLOG_IF(FATAL, (buf[0] & 0xFF) != SKIP_LIST_FIRST_BYTE)
      << "Skip list has the wrong magic number: " << std::hex << buf[0];

    // varint at byte 1 is the number of entries
    uint32_t num_entries;
    int len = utils::varint_decode_uint32((char *)buf, 1, &num_entries);

    SkipListPreRow pre_row;
    pre_row.Reset();
    // byte 1 + len is the start of skip list entries
    VarintIterator it((const char *)buf, 1 + len, num_entries);

    for (uint32_t entry_i = 0; entry_i < num_entries; entry_i++) {
      uint32_t previous_doc_id = it.Pop() + pre_row.pre_doc_id;
      off_t file_offset_of_docid_bag = it.Pop() + pre_row.doc_id_blob_off;
      off_t file_offset_of_tf_bag = it.Pop() + pre_row.tf_blob_off;
      off_t file_offset_of_pos_blob = it.Pop() + pre_row.pos_blob_off;
      int in_blob_index_of_pos_bag = it.Pop();
      off_t file_offset_of_offset_blob = it.Pop() + pre_row.off_blob_off;
      int in_blob_index_of_offset_bag = it.Pop();

      AddEntry(previous_doc_id, 
               file_offset_of_docid_bag, 
               file_offset_of_tf_bag, 
               file_offset_of_pos_blob, 
               in_blob_index_of_pos_bag, 
               file_offset_of_offset_blob,
               in_blob_index_of_offset_bag);

      pre_row.pre_doc_id = previous_doc_id;
      pre_row.doc_id_blob_off = file_offset_of_docid_bag;
      pre_row.tf_blob_off = file_offset_of_tf_bag;
      pre_row.pos_blob_off = file_offset_of_pos_blob;
      pre_row.off_blob_off = file_offset_of_offset_blob;
    }

    skip_list_bytes_ = 1 + len + it.PoppedBytes();
  }

  std::size_t GetSkipListBytes() {
    return skip_list_bytes_;
  }

  std::size_t GetDocIdBytes() {
    const SkipEntry &e = skip_table_[0];
    return e.file_offset_of_tf_bag - e.file_offset_of_docid_bag;
  }

  std::size_t GetTfBytes() {
    const SkipEntry &e = skip_table_[0];
    return e.file_offset_of_pos_blob - e.file_offset_of_tf_bag;
  }

  std::size_t GetPosBytes() {
    const SkipEntry &e = skip_table_[0];
    return e.file_offset_of_offset_blob - e.file_offset_of_pos_blob;
  }

  std::size_t GetOffsetBytes() {
    int n = skip_table_.size();
    if (n == 1) {
      return GetPosBytes(); //an estimate
    } else {
      std::size_t blob_size = skip_table_[1].file_offset_of_offset_blob 
        - skip_table_[0].file_offset_of_offset_blob;
      return blob_size * n;
    }
  }

  int NumEntries() const {
    return skip_table_.size();
  }

  int StartPostingIndex(int skip_interval) const {
    return skip_interval * PACK_SIZE;
  }

  const SkipEntry &operator [](std::size_t interval_idx) const {
    DLOG_IF(FATAL, interval_idx >= skip_table_.size())
      << "Trying to access skip entry out of bound!";

    return skip_table_[interval_idx];
  }

  // Made public for easier testing
  void AddEntry(const uint32_t previous_doc_id,   
                const off_t file_offset_of_docid_bag,
                const off_t file_offset_of_tf_bag,
                const off_t file_offset_of_pos_blob,
                const int in_blob_index_of_pos_bag,
                const off_t file_offset_of_offset_blob,
                const int in_blob_index_of_offset_bag) 
  {
    if (file_offset_of_docid_bag < 0) {
      LOG(FATAL) << "file_offset_of_docid_bag < 0 in posting list!";
    }
    if (file_offset_of_tf_bag < 0) {
      LOG(FATAL) << "file_offset_of_tf_bag < 0 in posting list!";
    }
    if (file_offset_of_pos_blob < 0) {
      LOG(FATAL) << "file_offset_of_pos_blob < 0 in posting list!";
    }
    if (in_blob_index_of_pos_bag < 0) {
      LOG(FATAL) << "in_blob_index_of_pos_bag < 0 in posting list!";
    }
    if (file_offset_of_offset_blob < 0) {
      LOG(FATAL) << "file_offset_of_offset_blob < 0 in posting list!";
    }
    if (in_blob_index_of_offset_bag < 0) {
      LOG(FATAL) << "in_blob_index_of_offset_bag < 0 in posting list!";
    }

    skip_table_.emplace_back( previous_doc_id,   
                              file_offset_of_docid_bag,
                              file_offset_of_tf_bag,
                              file_offset_of_pos_blob,
                              in_blob_index_of_pos_bag,
                              file_offset_of_offset_blob,
                              in_blob_index_of_offset_bag);
  }

  std::string ToStr() const {
    std::string ret;

    ret = "~~~~~~~~ skip list ~~~~~~~~\n";
    ret += "prev_doc_id; off_docid_bag; off_tf_bag; off_pos_blob; index_pos_bag; off_off_blob; index_offset_bag;\n";
    for (auto &row : skip_table_) {
      ret += row.ToStr() + "\n";
    }

    return ret;
  }

 private:
  std::size_t skip_list_bytes_;
  std::vector<SkipEntry> skip_table_;
};


class TermDictEntry {
 public:
  void Load(const char *buf) {
    int len;
    uint32_t data_size;

    len = utils::varint_decode_uint32(buf, 0, &format_);
    buf += len;

    len = utils::varint_decode_uint32(buf, 0, &doc_freq_);
    buf += len;

    len = utils::varint_decode_uint32(buf, 0, &data_size);
    buf += len;

    if (format_ == 0) {
      skip_list_.Load((uint8_t *)buf);
    } else {
      LOG(FATAL) << "Format not supported.";
    }
  }

  const int DocFreq() const {
    return doc_freq_;
  }

  const SkipList &GetSkipList() const {
    return skip_list_;
  }

 private:
  uint32_t format_;
  uint32_t doc_freq_;
  SkipList skip_list_;
};


inline std::string ProduceBitmap(const std::vector<std::string> &vec) {
  const int n = vec.size();
  const int n_full_chunks = n / 8;
  const int rem = n % 8;
  uint8_t bits;
  std::string ret;

  for (int chunk_i = 0; chunk_i < n_full_chunks; chunk_i++) {
    bits = 0;
    for (int offset = 0; offset < 8; offset++) {
      const int i = chunk_i * 8 + offset;
      if (vec[i].size() > 0) {
        bits = bits | (1 << (7 - offset));
      }
    }
    ret += utils::MakeString(bits);
  }

  if (rem > 0) {
    bits = 0;
    for (int offset = 0; offset < rem; offset++) {
      const int i = n_full_chunks * 8 + offset;
      if (vec[i].size() > 0) {
        bits = bits | (1 << (7 - offset));
      }
    }
    ret += utils::MakeString(bits);
  }

  return ret;
}


class BloomBoxWriter {
 public:
  BloomBoxWriter(std::size_t array_bytes) :array_bytes_(array_bytes) {}

  void Add(std::string bitarray) {
    LOG_IF(FATAL, 
        !(bitarray.size() == array_bytes_ || bitarray.size() == 0))
      << "Size of bitarray does not match the expected";
    bit_arrays_.push_back(bitarray);
    LOG_IF(FATAL, bit_arrays_.size() > PACK_ITEM_CNT)
      << "Too many items in the bloom box";
  }

  std::string Serialize() const {
    VarintBuffer buf;
    buf.Append(utils::MakeString(BLOOM_BOX_FIRST_BYTE));
    // won't be larger than 1 byte, because the max is 128
    buf.Append(bit_arrays_.size()); 
    buf.Append(ProduceBitmap(bit_arrays_));
    for (auto &s : bit_arrays_) {
      buf.Append(s);
    }

    return buf.Data();
  }

 private:
  std::vector<std::string> bit_arrays_;
  std::size_t array_bytes_;
};


// Iterate over bit arrays inside a bloom box
class BloomBoxIterator {
 public:
  void Reset(const uint8_t *buf, const int item_bytes) {
    header_bytes_ = 0;
    accessed_indices_.clear();
    buf_ = buf;
    item_bytes_ = item_bytes;
    Fill(buf_);
    initialized_ = true;
  }

  bool HasItem(std::size_t i) {
    return bitmap_[i];
  }

  const uint8_t *GetBitArray(int i) {
    if (HasItem(i) == false)
      return nullptr;
    else {
      accessed_indices_.insert(i);
      return item_buf_ + map_[i] * item_bytes_;
    }
  }

  int AccessedBytes() const {
    if (initialized_ == false) 
      return 0;
    else 
      return header_bytes_ + accessed_indices_.size() * item_bytes_;
  }

 private:
  void Fill(const uint8_t *buf) {
    DLOG_IF(FATAL, buf[0] != BLOOM_BOX_FIRST_BYTE)
      << "Wrong first byte for a bloom box"
      << std::hex << (char) buf[0] << "!=" << BLOOM_BOX_FIRST_BYTE;
    buf += 1;

    // n_items_ is the total number of bloom filters, including empty ones
    int len = utils::varint_decode_uint32((char *)buf, 0, &n_items_);
    buf += len;

    const int n_bitmap_bytes = utils::DivideRoundUp(n_items_, 8);
    DecodeBitmap(buf, n_bitmap_bytes);
    buf += n_bitmap_bytes;

    BuildMap();
    item_buf_ = buf;
    
    header_bytes_ = 1 + len + n_bitmap_bytes;
  }

  void BuildMap() {
    int physical_index = 0;
    for (std::size_t i = 0; i < n_items_; i++) {
      if (bitmap_[i] == true) {
        map_[i] = physical_index;
        physical_index++;
      } else {
        map_[i] = -1;
      }
    }
  }

  void DecodeBitmap(const uint8_t *buf, const int n_bitmap_bytes) {
    bool *bitmap = bitmap_;
    for (int i = 0; i < n_bitmap_bytes; i++) {
      DecodeBitmapByte(*buf, bitmap); 
      buf++;
      bitmap += 8;
    }
  }

  bool initialized_ = false;
  std::size_t header_bytes_ = 0;
  uint32_t n_items_;
  const uint8_t *buf_;
  bool bitmap_[PACK_ITEM_CNT];
  const uint8_t *item_buf_;
  int item_bytes_;
  int map_[PACK_ITEM_CNT]; // logical index to physical index;
  std::unordered_set<int> accessed_indices_;
};


// The offsets store in bloom skip lists should be relative to
// the start of the posting list.
class BloomSkipListWriter {
 public:
  BloomSkipListWriter(const std::vector<off_t> &bloom_box_offs)
    :bloom_box_offs_(bloom_box_offs) {}
  
  std::string Serialize() const {
    VarintBuffer buf;
    buf.Append(utils::MakeString(BLOOM_SKIP_LIST_FIRST_BYTE));
    buf.Append(bloom_box_offs_.size());

    std::vector<off_t> deltas = utils::EncodeDelta<off_t>(bloom_box_offs_);

    for (auto &val : deltas) {
      buf.Append(val);
    }

    return buf.Data();
  }

 private:
  std::vector<off_t> bloom_box_offs_;
};


class BloomSkipList {
 public:
  void Load(const uint8_t *buf) {
    DLOG_IF(FATAL, buf[0] != BLOOM_SKIP_LIST_FIRST_BYTE)
      << "First byte of bloom skip list not not right" << std::endl;
    buf++;

    num_of_bytes_ = 0;

    uint32_t n_entries;
    int len = utils::varint_decode_uint32((const char *)buf, 0, &n_entries);
    buf += len;

    VarintIterator it((const char *)buf, 0, n_entries);
    off_t prev = 0;
    while (it.IsEnd() == false) {
      off_t offset = prev + it.Pop();
      bloom_box_offs_.push_back(offset);
      prev = offset;
    }

    num_of_bytes_ += 1 + len; // 1 is for the magic byte
    num_of_bytes_ += it.PoppedBytes();
  }

  off_t operator [](int index) {
    return bloom_box_offs_[index];
  }

  std::size_t NumOfBytes() const {
    return num_of_bytes_;
  }

  std::size_t Size() const {
    return bloom_box_offs_.size(); 
  }

 private:
  std::size_t num_of_bytes_ = 0;
  std::vector<off_t> bloom_box_offs_;
};





#endif

