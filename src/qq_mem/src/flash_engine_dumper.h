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


struct PostingPackIndex {
  PostingPackIndex(int block_idx, int offset_idx)
    : packed_block_idx(block_idx), in_block_idx(offset_idx) {}

  int packed_block_idx;
  int in_block_idx;
};

class PostingPackIndexes {
 public:
  void AddRow(int block_idx, int offset) {
    locations_.emplace_back(block_idx, offset);
  }

  int NumRows() const {
    return locations_.size();
  }

  const PostingPackIndex & operator[] (int i) const {
    return locations_[i];
  }

 private:
  std::vector<PostingPackIndex> locations_;
};


class TermEntryPackWriter {
 public:
  TermEntryPackWriter(std::vector<PackedIntsWriter> writers, 
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

  PostingPackIndexes GetPostingPackIndexes() const {
    int val_index = 0;  
    PostingPackIndexes table;
    
    for (auto &size : posting_sizes_) {
      table.AddRow(val_index / PackedIntsWriter::PACK_SIZE, 
          val_index % PackedIntsWriter::PACK_SIZE);
      val_index += size;
    }

    return table;
  }

  TermEntryPackWriter GetPackWriter(bool do_delta) const {
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

    return TermEntryPackWriter(pack_writers, vints);
  }

  const std::vector<uint32_t> &Values() const {
    return values_;
  }

  const std::vector<int> &PostingSizes() const {
    return posting_sizes_;
  }

 protected:
  std::vector<uint32_t> EncodeDelta() const {
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


class GeneralFileDumper {
 public:
  GeneralFileDumper(const std::string path) {
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666); 
    if (fd_ == -1) 
      LOG(FATAL) << "Cannot open file: " << path;
  }

  off_t CurrentOffset() const {
    off_t off = lseek(fd_, 0, SEEK_CUR);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  off_t Dump(const std::string &data) {
    off_t start_byte = CurrentOffset();
    utils::Write(fd_, data.data(), data.size());
    return start_byte;
  }

  void Flush() const {
    fsync(fd_);
  }

  void Close() const {
    close(fd_);
  }

 protected:
  int fd_;
};


class FileDumper {
 public:
  FileDumper(const std::string path) {
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666); 
    if (fd_ == -1) 
      LOG(FATAL) << "Cannot open file: " << path;
  }

  PackFileOffsets Dump(const TermEntryPackWriter &writer) {
    std::vector<off_t> pack_offs = DumpPackedBlocks(writer.PackWriters());
    std::vector<off_t> vint_offs = DumpVInts(writer.VInts());

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


class TermDictFileDumper : public GeneralFileDumper {
 public:
  TermDictFileDumper(const std::string path) :GeneralFileDumper(path) {}

  off_t DumpSkipList(const uint32_t doc_freq, const std::string &skip_list) {
    return DumpEntry(0, doc_freq, skip_list);
  }

  off_t DumpFullPostingList(const uint32_t doc_freq, const std::string &posting_list) {
    return DumpEntry(1, doc_freq, posting_list);
  }

 private:
  off_t DumpEntry(const uint32_t format, const uint32_t doc_freq, 
      const std::string &data) 
  {
    off_t start_off = CurrentOffset();

    VarintBuffer buf;
    buf.Append(format); // format indicator: 0 with skip list, 1: with posting list data
    buf.Append(doc_freq);
    buf.Append(data.size());

    Dump(buf.Data());
    Dump(data);

    return start_off;
  }
};


struct SkipPostingFileOffset {
  SkipPostingFileOffset(off_t offset, int index)
    : block_file_offset(offset), in_block_index(index) {}

  off_t block_file_offset;
  int in_block_index;
};


// Absolute file offsets for posting 0, 128, 128*2, ..'s data
class SkipPostingFileOffsets {
 public:
  SkipPostingFileOffsets(const PostingPackIndexes &table, 
      const PackFileOffsets &file_offs) {
    for (int posting_index = 0; 
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

  const SkipPostingFileOffset &operator [](int i) const {
    return locations_[i];
  }

 private:
  std::vector<SkipPostingFileOffset> locations_;
};


class SkipListWriter {
 public:
  SkipListWriter(const SkipPostingFileOffsets &docid_file_offs,
           const SkipPostingFileOffsets &tf_file_offs,
           const SkipPostingFileOffsets &pos_file_offs,
           const SkipPostingFileOffsets &off_file_offs,
           const std::vector<uint32_t> doc_ids) 
    :docid_offs_(docid_file_offs),
     tf_offs_(tf_file_offs),
     pos_offs_(pos_file_offs),
     off_offs_(off_file_offs),
     doc_ids_(doc_ids)
  {}

  std::string Serialize() const {
    VarintBuffer buf;
    auto skip_doc_ids = GetSkipDocIds();

    if ( !(docid_offs_.Size() == tf_offs_.Size() && 
           tf_offs_.Size() == pos_offs_.Size() &&
           pos_offs_.Size() == off_offs_.Size() &&
           off_offs_.Size() == skip_doc_ids.size()) ) 
    {
      std::cout
        <<    docid_offs_.Size() << ", " 
        <<    tf_offs_.Size() << ", "
        <<    pos_offs_.Size() << ", "
        <<    off_offs_.Size() << ", "
        <<    skip_doc_ids.size() << std::endl;
      LOG(FATAL) << "Skip data is not uniform";
    }

    for (int i = 0; i < docid_offs_.Size(); i++) {
      AddRow(&buf, i, skip_doc_ids);
    }

    return buf.Data();
  }

 private:
  void AddRow(VarintBuffer *buf, int i, 
      const std::vector<uint32_t> skip_doc_ids) const {
    buf->Append(skip_doc_ids[i]);
    buf->Append(docid_offs_[i].block_file_offset);
    buf->Append(tf_offs_[i].block_file_offset);
    buf->Append(pos_offs_[i].block_file_offset);
    buf->Append(pos_offs_[i].in_block_index);
    buf->Append(off_offs_[i].block_file_offset);
  }

  std::vector<uint32_t> GetSkipDocIds() const {
    std::vector<uint32_t> doc_ids;
    for (int i = 0; i < doc_ids_.size(); i += SKIP_INTERVAL) {
      doc_ids.push_back(doc_ids_[i]); 
    }
    return doc_ids;
  }

  const SkipPostingFileOffsets &docid_offs_;
  const SkipPostingFileOffsets &tf_offs_;
  const SkipPostingFileOffsets &pos_offs_;
  const SkipPostingFileOffsets &off_offs_;
  const std::vector<uint32_t> doc_ids_;
};


struct SkipEntry {
  SkipEntry(const uint32_t doc_skip_in,   
            const off_t doc_file_offset_in,
            const off_t tf_file_offset_in,
            const off_t pos_file_offset_in,
            const int pos_in_block_index_in,
            const off_t off_file_offset_in)
    : doc_skip(doc_skip_in),
      doc_file_offset(doc_file_offset_in),
      tf_file_offset(tf_file_offset_in),
      pos_file_offset(pos_file_offset_in),
      pos_in_block_index(pos_in_block_index_in),
      off_file_offset(off_file_offset_in) {}
 
  uint32_t doc_skip;
  off_t doc_file_offset;
  off_t tf_file_offset;
  off_t pos_file_offset;
  int pos_in_block_index;
  off_t off_file_offset;
};

class SkipList {
 public:
  void Load(const uint8_t *buf, const int num_entries) {
    VarintIterator it((const char *)buf, 0, num_entries);

    for (int entry_i = 0; entry_i < num_entries; entry_i++) {
      uint32_t doc_skip = it.Pop();
      off_t doc_file_offset = it.Pop();
      off_t tf_file_offset = it.Pop();
      off_t pos_file_offset = it.Pop();
      int pos_in_block_index = it.Pop();
      off_t off_file_offset = it.Pop();
      AddEntry(doc_skip, doc_file_offset, tf_file_offset, 
          pos_file_offset, pos_in_block_index, off_file_offset);
    }
  }

  int NumEntries() const {
    return skip_table_.size();
  }

  const SkipEntry &operator [](int i) {
    return skip_table_[i];
  }

 private:
  void AddEntry(const uint32_t doc_skip,   
                const off_t doc_file_offset,
                const off_t tf_file_offset,
                const off_t pos_file_offset,
                const int pos_in_block_index,
                const off_t off_file_offset) 
  {
    skip_table_.emplace_back( doc_skip,   
                              doc_file_offset,
                              tf_file_offset,
                              pos_file_offset,
                              pos_in_block_index,
                              off_file_offset);
  }

  std::vector<SkipEntry> skip_table_;
};


class InvertedIndexDumper : public InvertedIndexQqMemDelta {
 public:
  InvertedIndexDumper(const std::string dump_dir_path)
    :position_dumper_(dump_dir_path + "/my.pos"),
     offset_dumper_(dump_dir_path + "/my.off"),
     termfreq_dumper_(dump_dir_path + "/my.tf")
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

    GeneralTermEntry docid_term_entry;
    GeneralTermEntry termfreq_term_entry;
    GeneralTermEntry position_term_entry;
    GeneralTermEntry offset_term_entry;

    while (posting_it.IsEnd() == false) {
      //doc id
      docid_term_entry.AddGroup(
          std::vector<uint32_t>{(uint32_t)posting_it.DocId()});

      //Term Freq
      termfreq_term_entry.AddGroup(
          std::vector<uint32_t>{(uint32_t)posting_it.TermFreq()});

      // Position
      position_term_entry.AddGroup(
          ExtractPositions(posting_it.PositionBegin().get()));

      // Offset
      offset_term_entry.AddGroup(
          ExtractOffsets(posting_it.OffsetPairsBegin().get()));

      posting_it.Advance();
    }


    SkipPostingFileOffsets pos_skip_offs = 
      DumpTermEntry(position_term_entry, &position_dumper_, true);
    SkipPostingFileOffsets off_skip_offs = 
      DumpTermEntry(offset_term_entry, &offset_dumper_, true);
    SkipPostingFileOffsets tf_skip_offs = 
      DumpTermEntry(termfreq_term_entry, &termfreq_dumper_, false);
  }

  SkipPostingFileOffsets DumpTermEntry(
      const GeneralTermEntry &term_entry, FileDumper *dumper, bool do_delta) {
    PackFileOffsets file_offs = dumper->Dump(term_entry.GetPackWriter(do_delta));
    PostingPackIndexes pack_indexes = term_entry.GetPostingPackIndexes();
    return SkipPostingFileOffsets(pack_indexes, file_offs);
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
  FileDumper termfreq_dumper_;
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




