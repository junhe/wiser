#ifndef FLASH_ENGINE_DUMPER_H
#define FLASH_ENGINE_DUMPER_H

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
constexpr int PACK_SIZE = PackedIntsWriter::PACK_SIZE;


struct PostingBagBlobIndex {
  PostingBagBlobIndex(int block_idx, int offset_idx)
    : blob_index(block_idx), in_blob_idx(offset_idx) {}

  int blob_index;
  int in_blob_idx;
};


inline std::vector<uint32_t> ExtractPositions(PopIteratorService *pos_it) {
  std::vector<uint32_t> positions;
  while (pos_it->IsEnd() == false) {
    positions.push_back(pos_it->Pop());  
  }
  return positions;
}

inline std::vector<uint32_t> ExtractOffsets(OffsetPairsIteratorService *iterator) {
  std::vector<uint32_t> offsets;
  while (iterator->IsEnd() != true) {
    OffsetPair pair;
    iterator->Pop(&pair);

    offsets.push_back(std::get<0>(pair));
    offsets.push_back(std::get<1>(pair));
  }
  return offsets;
}

inline std::vector<uint32_t> GetSkipPostingPreDocIds(const std::vector<uint32_t> &doc_ids) {
  std::vector<uint32_t> skip_pre_doc_ids{0}; // the first is always 0
  for (int skip_posting_i = SKIP_INTERVAL; 
      skip_posting_i < doc_ids.size(); 
      skip_posting_i += SKIP_INTERVAL) 
  {
    skip_pre_doc_ids.push_back(doc_ids[skip_posting_i - 1]); 
  }
  return skip_pre_doc_ids;
}


inline std::vector<uint32_t> EncodeDelta(const std::vector<uint32_t> &values) {
  uint32_t prev = 0;
  std::vector<uint32_t> vals;

  for (auto &v : values) {
    vals.push_back(v - prev);
    prev = v;
  }

  return vals;
}



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
  CozyBoxWriter(std::vector<PackedIntsWriter> writers, 
                     VIntsWriter vints)
    :pack_writers_(writers), vints_(vints) {}

  const std::vector<PackedIntsWriter> &PackWriters() const {
    return pack_writers_;
  }

  const VIntsWriter &VInts() const {
    return vints_;
  }

 private:
  std::vector<PackedIntsWriter> pack_writers_;
  VIntsWriter vints_;
};


class GeneralTermEntry {
 public:
  // Values can be positions, offsets, term frequencies
  void AddPostingBag(std::vector<uint32_t> values) {
    posting_bag_sizes_.push_back(values.size()); 

    for (auto &v : values) {
      values_.push_back(v);
    }
  }

  PostingBagBlobIndexes GetPostingBagIndexes() const {
    int val_index = 0;  
    PostingBagBlobIndexes table;
    
    for (auto &size : posting_bag_sizes_) {
      table.AddRow(val_index / PackedIntsWriter::PACK_SIZE, 
          val_index % PackedIntsWriter::PACK_SIZE);
      val_index += size;
    }

    return table;
  }

  CozyBoxWriter GetCozyBoxWriter(bool do_delta) const {
    const int pack_size = PackedIntsWriter::PACK_SIZE;
    const int n_packs = values_.size() / pack_size;
    const int n_remains = values_.size() % pack_size;

    std::vector<PackedIntsWriter> pack_writers(n_packs);
    VIntsWriter vints;

    std::vector<uint32_t> vals;
    if (do_delta) {
      vals = EncodeDelta(values_);
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

    return CozyBoxWriter(pack_writers, vints);
  }

  const std::vector<uint32_t> &Values() const {
    return values_;
  }

  const std::vector<int> &PostingBagSizes() const {
    return posting_bag_sizes_;
  }

 protected:
  // number of values in each posting
  std::vector<int> posting_bag_sizes_;
  std::vector<uint32_t> values_;
  uint32_t prev_ = 0;
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

  off_t Seek(off_t pos) {
    off_t off = lseek(fd_, pos, SEEK_SET);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  off_t SeekToEnd() {
    off_t off = lseek(fd_, 0, SEEK_END);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  off_t End() {
    off_t old = CurrentOffset();

    off_t end_off = lseek(fd_, 0, SEEK_END);
    if (end_off == -1)
      LOG(FATAL) << "Failed to get the ending offset.";

    Seek(old);

    return end_off;
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


class TermIndexDumper : public GeneralFileDumper {
 public:
  TermIndexDumper(const std::string &path) : GeneralFileDumper(path) {}

  // length of term, term, offset of entry in .tim
  void DumpEntry(Term term, off_t offset) {
    DumpUint32(term.size());
    Dump(term);
    DumpLong(offset);
  }
  
 private:
  void DumpLong(off_t val) {
    std::string data((char *)&val, sizeof(val));
    Dump(data);
  }

  void DumpUint32(uint32_t val) {
    std::string data((char *)&val, sizeof(val));
    Dump(data);
  }
};


class FileDumper : public GeneralFileDumper {
 public:
  FileDumper(const std::string path) : GeneralFileDumper(path) {}

  FileOffsetsOfBlobs Dump(const CozyBoxWriter &writer) {
    std::vector<off_t> pack_offs = DumpPackedBlocks(writer.PackWriters());
    std::vector<off_t> vint_offs = DumpVInts(writer.VInts());

    return FileOffsetsOfBlobs(pack_offs, vint_offs);
  }

  off_t Dump(const std::string &data) {
    return GeneralFileDumper::Dump(data);
  }

 protected:
  std::vector<off_t> DumpVInts(const VIntsWriter &varint_buf) {
    if (varint_buf.IntsSize() == 0) {
      return std::vector<off_t>{};
    } else {
      off_t start_byte = CurrentOffset();
      std::string data_buf = varint_buf.Serialize();

      utils::Write(fd_, data_buf.data(), data_buf.size());
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
    return start_byte;
  }
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
      locations_.emplace_back(file_offs.FileOffset(pack_id), in_blob_idx);
    }
  }

  int Size() const {
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

  std::string Serialize() const {
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
    buf.Append(n_rows);
    for (int i = 0; i < n_rows; i++) {
      AddRow(&buf, i, skip_pre_doc_ids);
    }

    return buf.Data();
  }

 private:
  void AddRow(VarintBuffer *buf, int i, 
      const std::vector<uint32_t> skip_pre_doc_ids) const {
    buf->Append(skip_pre_doc_ids[i]);
    buf->Append(docid_offs_[i].file_offset_of_blob);
    buf->Append(tf_offs_[i].file_offset_of_blob);
    buf->Append(pos_offs_[i].file_offset_of_blob);
    buf->Append(pos_offs_[i].in_blob_index);
    buf->Append(off_offs_[i].file_offset_of_blob);
    buf->Append(off_offs_[i].in_blob_index);
  }

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
};

class SkipList {
 public:
  void Load(const uint8_t *buf) {
    uint32_t num_entries;
    int len = utils::varint_decode_chars((char *)buf, 0, &num_entries);
    VarintIterator it((const char *)buf, len, num_entries);

    for (int entry_i = 0; entry_i < num_entries; entry_i++) {
      uint32_t previous_doc_id = it.Pop();
      off_t file_offset_of_docid_bag = it.Pop();
      off_t file_offset_of_tf_bag = it.Pop();
      off_t file_offset_of_pos_blob = it.Pop();
      int in_blob_index_of_pos_bag = it.Pop();
      off_t file_offset_of_offset_blob = it.Pop();
      int in_blob_index_of_offset_bag = it.Pop();
      AddEntry(previous_doc_id, 
               file_offset_of_docid_bag, 
               file_offset_of_tf_bag, 
               file_offset_of_pos_blob, 
               in_blob_index_of_pos_bag, 
               file_offset_of_offset_blob,
               in_blob_index_of_offset_bag);
    }
  }

  int NumEntries() const {
    return skip_table_.size();
  }

  int StartPostingIndex(int skip_interval) const {
    return skip_interval * PACK_SIZE;
  }

  const SkipEntry &operator [](int interval_idx) const {
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
    skip_table_.emplace_back( previous_doc_id,   
                              file_offset_of_docid_bag,
                              file_offset_of_tf_bag,
                              file_offset_of_pos_blob,
                              in_blob_index_of_pos_bag,
                              file_offset_of_offset_blob,
                              in_blob_index_of_offset_bag);
  }

 private:
  std::vector<SkipEntry> skip_table_;
};


class PostingListMetadata {
 public:
  PostingListMetadata(int doc_freq, const uint8_t *skip_list_buf) 
    :doc_freq_(doc_freq)
  {
    skip_list_.Load(skip_list_buf);
  }

  int NumPostings() const {
    return doc_freq_;
  }

 private:
  SkipList skip_list_;
  int doc_freq_;
};


class TermDictEntry {
 public:
  void Load(const char *buf) {
    int len;
    uint32_t data_size;

    len = utils::varint_decode_chars(buf, 0, &format_);
    buf += len;

    len = utils::varint_decode_chars(buf, 0, &doc_freq_);
    buf += len;

    len = utils::varint_decode_chars(buf, 0, &data_size);
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


class InvertedIndexDumperBase : public InvertedIndexQqMemDelta {
 public:
  void Dump() {
    for (auto it = index_.cbegin(); it != index_.cend(); it++) {
      // LOG(INFO) << "At '" << it->first << "'" << std::endl;
      DumpPostingList(it->first, it->second);
    }
  }
  virtual void DumpPostingList(
      const Term &term, const PostingListDelta &posting_list) = 0;

};


struct TermEntrySet {
    GeneralTermEntry docid;
    GeneralTermEntry termfreq;
    GeneralTermEntry position;
    GeneralTermEntry offset;
};

inline FileOffsetOfSkipPostingBags DumpTermEntry(
    const GeneralTermEntry &term_entry, FileDumper *dumper, bool do_delta) {
  FileOffsetsOfBlobs file_offs = dumper->Dump(term_entry.GetCozyBoxWriter(do_delta));
  PostingBagBlobIndexes pack_indexes = term_entry.GetPostingBagIndexes();
  return FileOffsetOfSkipPostingBags(pack_indexes, file_offs);
}


class VacuumInvertedIndexDumper : public InvertedIndexDumperBase {
 public:
  VacuumInvertedIndexDumper(const std::string dump_dir_path)
    :index_dumper_(dump_dir_path + "/my.vacuum"),
     fake_index_dumper_(dump_dir_path + "/fake.vacuum"),
     term_index_dumper_(dump_dir_path + "/my.tip")
  {}

  void DumpPostingList(const Term &term, 
      const PostingListDelta &posting_list) override {
    PostingListDeltaIterator posting_it = posting_list.Begin2();
    LOG(INFO) << "Dumping Posting List of " << term << std::endl;
    LOG(INFO) << "Number of postings: " << posting_it.Size() << std::endl;

    TermEntrySet entry_set;

    while (posting_it.IsEnd() == false) {
      LOG(INFO) << "DocId: " << posting_it.DocId() << std::endl;
      //doc id
      entry_set.docid.AddPostingBag(
          std::vector<uint32_t>{(uint32_t)posting_it.DocId()});

      //Term Freq
      entry_set.termfreq.AddPostingBag(
          std::vector<uint32_t>{(uint32_t)posting_it.TermFreq()});

      // Position
      entry_set.position.AddPostingBag(
          EncodeDelta(ExtractPositions(posting_it.PositionBegin().get())));

      // Offset
      entry_set.offset.AddPostingBag(
          EncodeDelta(ExtractOffsets(posting_it.OffsetPairsBegin().get())));

      posting_it.Advance();
    }

    off_t posting_list_start = index_dumper_.CurrentOffset();
    LOG(INFO) << "posting_list_start: " << posting_list_start;
    
    // Dump doc freq
    LOG(INFO) << "dumping doc freq: " << posting_list.Size();
    DumpVarint(posting_list.Size());

    // Dump skip list
    off_t skip_list_start = index_dumper_.CurrentOffset();
    LOG(INFO) << "skip_list_start: " << skip_list_start;
    int skip_list_est_size = EstimateSkipListBytes(skip_list_start, entry_set);
    LOG(INFO) << "skip_list_est_size: " << skip_list_est_size << std::endl;

    LOG(INFO) << "Dumping real skiplist...........................\n";
    // Dump doc id, term freq, ...
    SkipListWriter real_skiplist_writer = DumpTermEntrySet( 
        &index_dumper_, 
        skip_list_start + skip_list_est_size,
        entry_set,
        entry_set.docid.Values());

    index_dumper_.Seek(skip_list_start);

    std::string skip_list_data = real_skiplist_writer.Serialize();
    LOG(INFO) << "skip_list_real size: " << skip_list_data.size() << std::endl;
    if (skip_list_data.size() > skip_list_est_size) { 
      LOG(FATAL) << "Gap for skip list is too small.";
    } else {
      index_dumper_.Dump(skip_list_data); 
      index_dumper_.SeekToEnd();
    }

    term_index_dumper_.DumpEntry(term, posting_list_start);
  }

 private:
  int EstimateSkipListBytes(off_t skip_list_start, const TermEntrySet &entry_set) {
    LOG(INFO) << "Dumping fake skiplist...........................\n";
    SkipListWriter fake_skiplist_writer = DumpTermEntrySet( 
        &fake_index_dumper_, 
        skip_list_start + 1024*1024,
        entry_set,
        entry_set.docid.Values());

    return fake_skiplist_writer.Serialize().size();
  }

  SkipListWriter DumpTermEntrySet(
    FileDumper *file_dumper,
    off_t file_offset,
    const TermEntrySet &entry_set,
    const std::vector<uint32_t> &doc_ids) 
  {
    file_dumper->Seek(file_offset);

    FileOffsetOfSkipPostingBags docid_skip_offs = 
      DumpTermEntry(entry_set.docid, file_dumper, true);
    FileOffsetOfSkipPostingBags tf_skip_offs = 
      DumpTermEntry(entry_set.termfreq, file_dumper, false);
    FileOffsetOfSkipPostingBags pos_skip_offs = 
      DumpTermEntry(entry_set.position, file_dumper, false);
    FileOffsetOfSkipPostingBags off_skip_offs = 
      DumpTermEntry(entry_set.offset, file_dumper, false);

    return SkipListWriter(docid_skip_offs, tf_skip_offs, 
        pos_skip_offs, off_skip_offs, entry_set.docid.Values());
  }


  void DumpVarint(uint32_t val) {
    VarintBuffer buf;
    buf.Append(val);
    index_dumper_.Dump(buf.Data()); 
  }

  FileDumper index_dumper_;
  FileDumper fake_index_dumper_;

  TermIndexDumper term_index_dumper_;
};



class FlashEngineDumper {
 public:
  FlashEngineDumper(const std::string dump_dir_path)
    :inverted_index_(dump_dir_path),
     dump_dir_path_(dump_dir_path)
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
      LOG(INFO) << "Adding ... " << doc_info.ToStr();
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

  void DumpInvertedIndex() {
    inverted_index_.Dump();
    doc_store_.Dump(dump_dir_path_ + "/my.fdx", dump_dir_path_ + "/my.fdt");
  }

 private:
  int next_doc_id_ = 0;
  std::string dump_dir_path_;

  FlashDocStoreDumper doc_store_;
  VacuumInvertedIndexDumper inverted_index_;
  DocLengthStore doc_lengths_;
  SimpleHighlighter highlighter_;
  Bm25Similarity similarity_;

  int NextDocId() {
    return next_doc_id_++;
  }
};

#endif
