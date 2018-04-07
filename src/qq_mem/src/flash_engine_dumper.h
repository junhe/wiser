#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <iostream>

#include "qq_mem_engine.h"
#include "packed_value.h"
#include "compression.h"

constexpr int SKIP_INTERVAL = PackedIntsWriter::PACK_SIZE;

// Provide functions to copy values to pack writer and VInts
class TermEntryBase {
 public:
  const std::vector<uint32_t> &Values() const {
    return values_;
  }

  const VarintBuffer &VInts() const {
    return vints_;
  }

  const std::vector<PackedIntsWriter> &PackWriters() const {
    return pack_writers_;
  }

 protected:
  void Fill() {
    const int pack_size = PackedIntsWriter::PACK_SIZE;
    const int n_packs = values_.size() / pack_size;
    const int n_remains = values_.size() % pack_size;

    pack_writers_.resize(n_packs);
    for (int pack_i = 0; pack_i < n_packs; pack_i++) {
      for (int offset = 0; offset < pack_size; offset++) {
        int value_idx = pack_i * pack_size + offset;
        pack_writers_[pack_i].Add(values_[value_idx]);
      }
    }
    
    for (int i = n_packs * pack_size; i < values_.size(); i++) {
      vints_.Append(values_[i]);
    }
  }

  std::vector<uint32_t> values_;
  std::vector<PackedIntsWriter> pack_writers_;
  VarintBuffer vints_;
};


class OffsetTermEntry :public TermEntryBase {
 public:
  OffsetTermEntry(CompressedPairIterator iterator) {
    int prev_off = 0;
    int delta;
    while (iterator.IsEnd() != true) {
      OffsetPair pair;
      iterator.Pop(&pair);

      uint32_t off1, off2;
      off1 = std::get<0>(pair);
      off2 = std::get<1>(pair);

      values_.push_back(off1 - prev_off);
      values_.push_back(off2 - off1);

      prev_off = off2;
    }

    Fill();
  }
};


class PositionTermEntry :public TermEntryBase {
 public:
  PositionTermEntry(PopIteratorService *pos_iter) {
    int prev_pos = 0;
    while (pos_iter->IsEnd() != true) {
      uint32_t pos = pos_iter->Pop();
      values_.push_back(pos - prev_pos);
      prev_pos = pos;
    }

    Fill();
  }
};


struct PostingLocation {
  PostingLocation(int block_idx, int offset_idx)
    : packed_block_idx(block_idx), in_block_idx(offset_idx) {}

  int packed_block_idx;
  int in_block_idx;
};

class PostingLocations {
 public:
  void AddRow(int block_idx, int offset) {
    locations_.emplace_back(block_idx, offset);
  }

  int NumRows() const {
    return locations_.size();
  }

  const PostingLocation & operator[] (int i) const {
    return locations_[i];
  }

 private:
  std::vector<PostingLocation> locations_;
};


class TermEntryContainer {
 public:
  TermEntryContainer(std::vector<PackedIntsWriter> writers, 
                     VarintBuffer vints)
    :pack_writers_(writers), vints_(vints) {}

  const std::vector<PackedIntsWriter> &PackWriters() const {
    return pack_writers_;
  }

  const VarintBuffer &VInts() const {
    return vints_;
  }

 private:
  std::vector<PackedIntsWriter> pack_writers_;
  VarintBuffer vints_;
};


class GeneralTermEntry {
 public:
  // Values can be positions, offsets, term frequencies
  void AddGroup(std::vector<uint32_t> values) {
    posting_sizes_.push_back(values.size()); 

    for (auto &v : values) {
      values_.push_back(v);
    }
  }

  PostingLocations GetPostingLocations() const {
    int val_index = 0;  
    PostingLocations table;
    
    for (auto &size : posting_sizes_) {
      table.AddRow(val_index / PackedIntsWriter::PACK_SIZE, 
          val_index % PackedIntsWriter::PACK_SIZE);
      val_index += size;
    }

    return table;
  }

  TermEntryContainer GetContainer(bool do_delta) {
    const int pack_size = PackedIntsWriter::PACK_SIZE;
    const int n_packs = values_.size() / pack_size;
    const int n_remains = values_.size() % pack_size;

    std::vector<PackedIntsWriter> pack_writers(n_packs);
    VarintBuffer vints;

    std::vector<uint32_t> vals;
    if (do_delta) {
      vals = EncodeDelta();
    } else {
      vals = values_;
    }

    for (int pack_i = 0; pack_i < n_packs; pack_i++) {
      for (int offset = 0; offset < pack_size; offset++) {
        int value_idx = pack_i * pack_size + offset;
        pack_writers[pack_i].Add(vals[value_idx]);
      }
    }
    
    for (int i = n_packs * pack_size; i < vals.size(); i++) {
      vints.Append(vals[i]);
    }

    return TermEntryContainer(pack_writers, vints);
  }

  const std::vector<uint32_t> &Values() const {
    return values_;
  }

  const std::vector<int> &PostingSizes() const {
    return posting_sizes_;
  }

 protected:
  std::vector<uint32_t> EncodeDelta() {
    uint32_t prev = 0;
    std::vector<uint32_t> vals;
    for (auto &v : values_) {
      vals.push_back(v - prev);
      prev = v;
    }

    return vals;
  }

  // number of values in each posting
  std::vector<int> posting_sizes_;
  std::vector<uint32_t> values_;
  uint32_t prev_ = 0;
};




class PackFileOffsets {
 public:
  PackFileOffsets(std::vector<off_t> pack_offs, std::vector<off_t> vint_offs)
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

  off_t FileOffset(int pack_index) const {
    const int n_packs = pack_offs_.size();
    if (pack_index < n_packs) {
      return pack_offs_[pack_index];
    } else if (pack_index == n_packs) {
      if (vint_offs_.size() == 0)
        LOG(FATAL) << "vint_offs_.size() should not be 0.";
      return vint_offs_[0];
    } else {
      LOG(FATAL) << "pack_index is too large.";
    }
  }

 private:
  std::vector<off_t> pack_offs_;
  std::vector<off_t> vint_offs_;
};

class FileDumper {
 public:
  FileDumper(const std::string path) {
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666); 
    if (fd_ == -1) 
      LOG(FATAL) << "Cannot open file: " << path;
  }

  PackFileOffsets Dump(const TermEntryContainer &container) {
    std::vector<off_t> pack_offs = DumpPackedBlocks(container.PackWriters());
    std::vector<off_t> vint_offs = DumpVInts(container.VInts());

    return PackFileOffsets(pack_offs, vint_offs);
  }

  off_t CurrentOffset() const {
    off_t off = lseek(fd_, 0, SEEK_CUR);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  void Flush() const {
    fsync(fd_);
  }

  void Close() const {
    close(fd_);
  }

 protected:
  std::vector<off_t> DumpVInts(const VarintBuffer &varint_buf) {
    if (varint_buf.Size() == 0) {
      return std::vector<off_t>{};
    } else {
      off_t start_byte = CurrentOffset();
      utils::Write(fd_, varint_buf.Data().data(), varint_buf.Size());
      return std::vector<off_t>{start_byte};
    }
  }

  std::vector<off_t> DumpPackedBlocks(
      const std::vector<PackedIntsWriter> &pack_writers) {
    std::vector<off_t> offs;

    for (auto &writer : pack_writers) {
      offs.push_back(DumpPackedBlock(writer));
    }

    return offs;
  }

  off_t DumpPackedBlock(const PackedIntsWriter &writer) {
    off_t start_byte = CurrentOffset();
    std::string data = writer.Serialize();      
    utils::Write(fd_, data.data(), data.size());
  }

  int fd_;
};


struct SkipPostingLocation {
  SkipPostingLocation(off_t offset, int index)
    : block_file_offset(offset), in_block_index(index) {}

  off_t block_file_offset;
  int in_block_index;
};


class SkipPostingLocations {
 public:
  SkipPostingLocations(const PostingLocations &table, 
      const PackFileOffsets &file_offs) {
    for (int posting_index = SKIP_INTERVAL; 
        posting_index < table.NumRows(); 
        posting_index += SKIP_INTERVAL) 
    {
      const int pack_id = table[posting_index].packed_block_idx;
      const int in_block_idx = table[posting_index].in_block_idx;
      locations_.emplace_back(file_offs.FileOffset(pack_id), in_block_idx);
    }
  }

  int Size() const {
    return locations_.size();
  }

  const SkipPostingLocation &operator [](int i) const {
    return locations_[i];
  }

 private:
  std::vector<SkipPostingLocation> locations_;
};


class InvertedIndexDumper : public InvertedIndexQqMemDelta {
 public:
  InvertedIndexDumper(const std::string dump_dir_path)
    :position_dumper_(dump_dir_path + "/my.pos"),
     offset_dumper_(dump_dir_path + "/my.off")
  {}

  void Dump(const std::string dir_path) {
    // std::cout << "Dumping Inverted Index...." << std::endl; 

    for (auto it = index_.cbegin(); it != index_.cend(); it++) {
      // std::cout << "At '" << it->first << "'" << std::endl;
      DumpPostingList(it->second);
    }
  }

  void DumpPostingList(const PostingListDelta &posting_list) {
    PostingListDeltaIterator posting_it = posting_list.Begin2();
    // std::cout << "Dumping One Posting List ...." << std::endl;
    // std::cout << "Number of postings: " << posting_it.Size() << std::endl;

    std::vector<uint32_t> doc_ids, term_freqs;
    GeneralTermEntry position_term_entry;
    GeneralTermEntry offset_term_entry;

    while (posting_it.IsEnd() == false) {
      // std::cout << "DocId: " << posting_it.DocId() << std::endl;

      doc_ids.push_back(posting_it.DocId());
      term_freqs.push_back(posting_it.TermFreq());

      // Position
      position_term_entry.AddGroup(
          ExtractPositions(posting_it.PositionBegin().get()));

      // Offset
      offset_term_entry.AddGroup(
          ExtractOffsets(posting_it.OffsetPairsBegin().get()));

      posting_it.Advance();
    }

    PackFileOffsets pos_file_offs = position_dumper_.Dump(
        position_term_entry.GetContainer(true));
    PackFileOffsets offset_file_offs = offset_dumper_.Dump(
        offset_term_entry.GetContainer(true));
  }

  std::vector<uint32_t> ExtractPositions(PopIteratorService *pos_it) {
    std::vector<uint32_t> positions;
    while (pos_it->IsEnd() == false) {
      positions.push_back(pos_it->Pop());  
    }
    return positions;
  }

  std::vector<uint32_t> ExtractOffsets(OffsetPairsIteratorService *iterator) {
    std::vector<uint32_t> offsets;
    while (iterator->IsEnd() != true) {
      OffsetPair pair;
      iterator->Pop(&pair);

      offsets.push_back(std::get<0>(pair));
      offsets.push_back(std::get<1>(pair));
    }
    return offsets;
  }

 private:
  FileDumper position_dumper_;
  FileDumper offset_dumper_;
};


class FlashEngineDumper {
 public:
  FlashEngineDumper(const std::string dump_dir_path)
    :inverted_index_(dump_dir_path) 
  {}

  // colum 2 should be tokens
  int LoadLocalDocuments(const std::string &line_doc_path, 
      int n_rows, const std::string format) {
    int ret;
    std::unique_ptr<LineDocParserService> parser;

    if (format == "TOKEN_ONLY") {
      parser.reset(new LineDocParserToken(line_doc_path, n_rows));
    } else if (format == "WITH_OFFSETS") {
      parser.reset(new LineDocParserOffset(line_doc_path, n_rows));
    } else if (format == "WITH_POSITIONS") {
      parser.reset(new LineDocParserPosition(line_doc_path, n_rows));
    } else {
      throw std::runtime_error("Format " + format + " is not supported");
    }

    DocInfo doc_info;
    while (parser->Pop(&doc_info)) {
      AddDocument(doc_info); 
      if (parser->Count() % 10000 == 0) {
        std::cout << "Indexed " << parser->Count() << " documents" << std::endl;
      }
    }

    LOG(WARNING) << "Number of terms in inverted index: " << TermCount();
    return ret;
  }

  void AddDocument(const DocInfo doc_info) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, doc_info.Body());
    inverted_index_.AddDocument(doc_id, doc_info);
    doc_lengths_.AddLength(doc_id, doc_info.BodyLength()); // TODO modify to count on offsets?
    similarity_.Reset(doc_lengths_.GetAvgLength());
  }

  std::string GetDocument(const DocIdType &doc_id) {
    return doc_store_.Get(doc_id);
  }

  int TermCount() const {
    return inverted_index_.Size();
  }

  std::map<std::string, int> PostinglistSizes(const TermList &terms) {
    return inverted_index_.PostinglistSizes(terms);
  }

  void Dump(std::string dir_path) {
  }

  void DumpInvertedIndex(const std::string dir_path) {
    inverted_index_.Dump(dir_path);
  }

 private:
  int next_doc_id_ = 0;
  CompressedDocStore doc_store_;
  InvertedIndexDumper inverted_index_;
  DocLengthStore doc_lengths_;
  SimpleHighlighter highlighter_;
  Bm25Similarity similarity_;

  int NextDocId() {
    return next_doc_id_++;
  }
};




