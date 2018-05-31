#ifndef FLASH_CONTAINERS_H
#define FLASH_CONTAINERS_H


#define SKIP_LIST_FIRST_BYTE 0xA3
#define POSTING_LIST_FIRST_BYTE 0xF4

constexpr int SKIP_INTERVAL = PACK_ITEM_CNT;
constexpr int PACK_SIZE = PACK_ITEM_CNT;


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




#endif

