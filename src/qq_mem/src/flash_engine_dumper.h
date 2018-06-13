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
#include "flash_containers.h"
#include "file_dumper.h"


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


// n_pages_of_zone     posting_list_start 
// |..................|......................................|
//    16 bits             48bits
inline off_t EncodePrefetchZoneAndOffset(
    const uint32_t n_pages_of_zone, const off_t posting_list_start) {
  DLOG_IF(FATAL, (((uint64_t) n_pages_of_zone) << 48) >> 48 != n_pages_of_zone)
    << "n_pages_of_zone is too large to be encoded.";
  return (((uint64_t) n_pages_of_zone) << 48) | posting_list_start;
}

inline void DecodePrefetchZoneAndOffset(
    const off_t offset_from_term_index, uint32_t *n_pages_of_zone, off_t *posting_list_start) {
  *n_pages_of_zone = ((uint64_t) offset_from_term_index) >> 48;
  constexpr uint64_t mask = (((~(uint64_t) 0) << 16) >> 16); // 0b0000 111..111   48 1s
  *posting_list_start = ((uint64_t)offset_from_term_index) & mask;
}

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
      table.AddRow(val_index / PACK_ITEM_CNT, val_index % PACK_ITEM_CNT);
      val_index += size;
    }

    return table;
  }

  int NumBags() const {
    return posting_bag_sizes_.size();
  }

  CozyBoxWriter GetCozyBoxWriter(bool do_delta) const {
    const int pack_size = PACK_ITEM_CNT;
    const int n_packs = values_.size() / pack_size;

    std::vector<LittlePackedIntsWriter> pack_writers(n_packs);
    VIntsWriter vints;

    std::vector<uint32_t> vals;
    if (do_delta) {
      vals = utils::EncodeDelta<uint32_t>(values_);
    } else {
      vals = values_;
    }

    for (int pack_i = 0; pack_i < n_packs; pack_i++) {
      for (int offset = 0; offset < pack_size; offset++) {
        int value_idx = pack_i * pack_size + offset;
        pack_writers[pack_i].Add(vals[value_idx]);
      }
    }
    
    for (std::size_t i = n_packs * pack_size; i < vals.size(); i++) {
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





struct ResultOfDumpingTermEntrySet {
  ResultOfDumpingTermEntrySet(SkipListWriter writer, off_t offset)
    :skip_list_writer(writer), pos_start_offset(offset)
  {}

  SkipListWriter skip_list_writer;
  off_t pos_start_offset;
};


struct ResultOfDumpingTermEntrySetWithBloom {
  ResultOfDumpingTermEntrySetWithBloom(SkipListWriter writer, off_t offset, 
      off_t bloom_off)
    :skip_list_writer(writer), pos_start_offset(offset),
     bloom_section_offset(bloom_off)
  {}

  SkipListWriter skip_list_writer;
  off_t pos_start_offset;
  off_t bloom_section_offset;
};




class BloomFilterColumnWriter {
 public:
  BloomFilterColumnWriter(std::size_t array_bytes)
    : array_bytes_(array_bytes) {}

  void AddPostingBag(const std::string &bit_array) {
    bit_arrays_.push_back(bit_array);
  }

  std::vector<off_t> Dump(FileDumper *file_dumper) const {
    std::vector<BloomBoxWriter> writers = GetWriters();
    std::vector<off_t> ret;

    for (auto &writer : writers) {
      ret.push_back(file_dumper->CurrentOffset());
      std::string data = writer.Serialize();
      file_dumper->Dump(data);  
    }

    return ret;
  }

  std::vector<BloomBoxWriter> GetWriters() const {
    const std::size_t n = bit_arrays_.size();
    std::size_t index = 0;
    std::vector<BloomBoxWriter> ret;

    while (index < n) {
      const int rem = n - index;
      const int box_size = rem < PACK_ITEM_CNT? rem : PACK_ITEM_CNT;

      BloomBoxWriter box_writer(array_bytes_);
      for (int i = 0; i < box_size; i++) {
        box_writer.Add(bit_arrays_[index + i]); 
      }
      ret.push_back(box_writer);

      index += box_size;
    }

    return ret;
  }

 private:
  std::size_t array_bytes_;
  std::vector<std::string> bit_arrays_;
};


inline std::vector<off_t> GetSerializedOffsets(
    const BloomFilterColumnWriter &col_writer)
{
  std::vector<off_t> ret;
  off_t cur_off = 0;

  std::vector<BloomBoxWriter> writers = col_writer.GetWriters();
  for (auto &writer : writers) {
    ret.push_back(cur_off);
    cur_off += writer.Serialize().size();
  }

  return ret;
}


inline std::size_t GetBloomSkipListSize(std::vector<off_t> box_offs) {
  BloomSkipListWriter writer(box_offs);
  return writer.Serialize().size();
}

inline std::size_t EstimateBloomSkipListSize(
    const off_t start_off,
    const std::vector<off_t> &box_offs) 
{
  std::vector<off_t> offs = box_offs;   
  for (auto &off : offs) {
    off += start_off;
  }

  return GetBloomSkipListSize(offs);
}


inline std::vector<off_t> GetRelativeOffsets(const off_t posting_list_start,
    const std::vector<off_t> &offs) 
{
  std::vector<off_t> ret = offs;
  for (auto &off : ret) {
    off -= posting_list_start;
  }

  return ret;
}


// The contents of a posting list
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


class VacuumInvertedIndexDumper : public InvertedIndexQqMemDelta {
 public:
  VacuumInvertedIndexDumper(const std::string dump_dir_path)
    :index_dumper_(dump_dir_path + "/my.vacuum"),
     fake_index_dumper_(dump_dir_path + "/fake.vacuum"),
     term_index_dumper_(dump_dir_path + "/my.tip"),
     bloom_store_begin_(nullptr),
     bloom_store_end_(nullptr)
  {}

  void Dump() {
    DumpHeader();

    int cnt = 0; 
    for (auto it = index_.cbegin(); it != index_.cend(); it++) {
      // LOG(INFO) << "At '" << it->first << "'" << std::endl;
      DumpPostingList(it->first, it->second);
      cnt++;
      if (cnt % 10000 == 0) {
        std::cout << "Posting list dumpped: " << cnt << std::endl;
      }
    }
  }

  void DumpHeader() {
    index_dumper_.Dump(utils::MakeString(VACUUM_FIRST_BYTE));

    if (bloom_store_begin_ == nullptr) {
      DumpVarint(0);
      DumpVarint(0);
      DumpVarint(0);
      index_dumper_.Dump(utils::SerializeFloat(0));
    } else {
      DumpVarint(1); // use bloom filter
      DumpVarint(bloom_store_begin_->BitArrayBytes());
      DumpVarint(bloom_store_begin_->ExpectedEntries());
      index_dumper_.Dump(utils::SerializeFloat(bloom_store_begin_->Ratio()));
    }

    if (bloom_store_end_ == nullptr) {
      DumpVarint(0);
      DumpVarint(0);
      DumpVarint(0);
      index_dumper_.Dump(utils::SerializeFloat(0));
    } else {
      DumpVarint(1); // use bloom filter
      DumpVarint(bloom_store_end_->BitArrayBytes());
      DumpVarint(bloom_store_end_->ExpectedEntries());
      index_dumper_.Dump(utils::SerializeFloat(bloom_store_end_->Ratio()));
    }

    index_dumper_.Seek(100);
  }

  // Only for testing at this time
  void SeekFileDumper(off_t pos) {
    index_dumper_.Seek(pos);
    fake_index_dumper_.Seek(pos);
  }

  void SetBloomStore(BloomFilterStore *bloom_store) {
    bloom_store_end_ = bloom_store;
  }

  void DumpPostingList(const Term &term, 
      const PostingListDelta &posting_list) {
    if (bloom_store_end_ == nullptr) 
      DumpPostingListNoBloom(term, posting_list);
    else
      DumpPostingListWithBloom(term, posting_list);
  }

  void DumpPostingListNoBloom(const Term &term, 
      const PostingListDelta &posting_list) {
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
          utils::EncodeDelta<uint32_t>(ExtractPositions(posting_it.PositionBegin().get())));

      // Offset
      entry_set.offset.AddPostingBag(
          utils::EncodeDelta<uint32_t>(ExtractOffsets(posting_it.OffsetPairsBegin().get())));

      posting_it.Advance();
    }

    off_t posting_list_start = index_dumper_.CurrentOffset();

    // Dump magic number
    index_dumper_.Dump(utils::MakeString(POSTING_LIST_FIRST_BYTE));
    
    // Dump doc freq
    DumpVarint(posting_list.Size());

    // Bloom skip list
    off_t bloom_pointer_location = index_dumper_.CurrentOffset();
    DumpVarint(0); 
    index_dumper_.Seek(bloom_pointer_location + 4);

    // Dump skip list
    off_t skip_list_start = index_dumper_.CurrentOffset();
    std::size_t skip_list_est_size = EstimateSkipListBytes(skip_list_start, entry_set);

    // Dump doc id, term freq, ...
    ResultOfDumpingTermEntrySet real_result = DumpTermEntrySet( 
        &index_dumper_, 
        skip_list_start + skip_list_est_size,
        entry_set,
        entry_set.docid.Values());

    std::string skip_list_data = real_result.skip_list_writer.Serialize();

    LOG_IF(FATAL, skip_list_data.size() > skip_list_est_size)  
        << "DATA CORRUPTION!!! Gap for skip list is too small. skip list real size: " 
        << skip_list_data.size() 
        << " skip est size: " << skip_list_est_size;

    index_dumper_.Seek(skip_list_start);
    index_dumper_.Dump(skip_list_data); 
    index_dumper_.SeekToEnd();

    // Dump to .tip
    // Calculate the prefetch zone size!
    uint32_t n_pages_of_prefetch_zone = 
      (real_result.pos_start_offset - posting_list_start) / 4096;

    term_index_dumper_.DumpEntry(term, 
        EncodePrefetchZoneAndOffset(n_pages_of_prefetch_zone, posting_list_start));
  }

  void DumpPostingListWithBloom(const Term &term, 
      const PostingListDelta &posting_list) {
    PostingListDeltaIterator posting_it = posting_list.Begin2();
    LOG(INFO) << "Dumping Posting List of " << term << std::endl;
    LOG(INFO) << "Number of postings: " << posting_it.Size() << std::endl;

    const BloomFilterCases &bloom_cases = bloom_store_end_->Lookup(term);
    auto bloom_iter = bloom_cases.Begin();

    BloomFilterColumnWriter bloom_col_writer(bloom_store_end_->BitArrayBytes());

    TermEntrySet entry_set;

    while (posting_it.IsEnd() == false) {
      LOG(INFO) << "DocId: " << posting_it.DocId() << std::endl;

      LOG_IF(FATAL, posting_it.DocId() != bloom_iter->doc_id)
        << "doc IDs do not match";

      //doc id
      entry_set.docid.AddPostingBag(
          std::vector<uint32_t>{(uint32_t)posting_it.DocId()});

      //Term Freq
      entry_set.termfreq.AddPostingBag(
          std::vector<uint32_t>{(uint32_t)posting_it.TermFreq()});

      //Bloom filters
      bloom_col_writer.AddPostingBag(bloom_iter->blm.BitArray());

      // Position
      entry_set.position.AddPostingBag(
          utils::EncodeDelta<uint32_t>(ExtractPositions(posting_it.PositionBegin().get())));

      // Offset
      entry_set.offset.AddPostingBag(
          utils::EncodeDelta<uint32_t>(ExtractOffsets(posting_it.OffsetPairsBegin().get())));

      posting_it.Advance();
      bloom_iter++;
    }
    LOG_IF(FATAL, bloom_iter != bloom_cases.End())
      << "bloom iter is not at the end";

    off_t posting_list_start = index_dumper_.CurrentOffset();

    // Dump magic number
    index_dumper_.Dump(utils::MakeString(POSTING_LIST_FIRST_BYTE));
    
    // Dump doc freq
    DumpVarint(posting_list.Size());

    // Reserve space for the location of bloom skip list
    off_t bloom_pointer_location = index_dumper_.CurrentOffset();
    index_dumper_.Seek(bloom_pointer_location + 4);

    // Estimate skip list size
    off_t skip_list_start = index_dumper_.CurrentOffset();
    std::size_t skip_list_est_size = EstimateSkipListBytes(
        skip_list_start, entry_set);

    // Dump doc id, term freq, ...
    ResultOfDumpingTermEntrySetWithBloom real_result = DumpTermEntrySetWithBloom( 
        posting_list_start,
        &index_dumper_, 
        skip_list_start + skip_list_est_size,
        entry_set,
        bloom_col_writer);

    std::string skip_list_data = real_result.skip_list_writer.Serialize();

    LOG_IF(FATAL, skip_list_data.size() > skip_list_est_size)  
        << "DATA CORRUPTION!!! Gap for skip list is too small. skip list real size: " 
        << skip_list_data.size() 
        << " skip est size: " << skip_list_est_size;

    index_dumper_.Seek(skip_list_start);
    index_dumper_.Dump(skip_list_data); 

    index_dumper_.Seek(bloom_pointer_location);
    DumpVarint(real_result.bloom_section_offset - posting_list_start);

    index_dumper_.SeekToEnd();

    // Dump to .tip
    // Calculate the prefetch zone size!
    uint32_t n_pages_of_prefetch_zone = 
      (real_result.pos_start_offset - posting_list_start) / 4096;

    term_index_dumper_.DumpEntry(term, 
        EncodePrefetchZoneAndOffset(n_pages_of_prefetch_zone, posting_list_start));
  }

 private:
  int EstimateSkipListBytes(off_t skip_list_start, const TermEntrySet &entry_set) {
    LOG(INFO) << "Dumping fake skiplist...........................\n";
    ResultOfDumpingTermEntrySet fake_result = DumpTermEntrySet( 
        &fake_index_dumper_, 
        skip_list_start + 512*1024,
        entry_set,
        entry_set.docid.Values());

    return fake_result.skip_list_writer.Serialize().size();
  }

  ResultOfDumpingTermEntrySet DumpTermEntrySet(
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

    off_t pos_start_offset = file_dumper->CurrentOffset();

    FileOffsetOfSkipPostingBags pos_skip_offs = 
      DumpTermEntry(entry_set.position, file_dumper, false);
    FileOffsetOfSkipPostingBags off_skip_offs = 
      DumpTermEntry(entry_set.offset, file_dumper, false);

    return ResultOfDumpingTermEntrySet(
        SkipListWriter(docid_skip_offs, tf_skip_offs, 
                       pos_skip_offs,   off_skip_offs, 
                       entry_set.docid.Values()),
        pos_start_offset);
  }

  ResultOfDumpingTermEntrySetWithBloom DumpTermEntrySetWithBloom(
    off_t posting_list_start,
    FileDumper *file_dumper,
    off_t file_offset,
    const TermEntrySet &entry_set,
    const BloomFilterColumnWriter &bloom_writer)
  {
    file_dumper->Seek(file_offset);

    FileOffsetOfSkipPostingBags docid_skip_offs = 
      DumpTermEntry(entry_set.docid, file_dumper, true);
    FileOffsetOfSkipPostingBags tf_skip_offs = 
      DumpTermEntry(entry_set.termfreq, file_dumper, false);

    off_t bloom_start_offset = file_dumper->CurrentOffset();
    DumpBloom(posting_list_start, file_dumper, bloom_writer);

    off_t pos_start_offset = file_dumper->CurrentOffset();

    FileOffsetOfSkipPostingBags pos_skip_offs = 
      DumpTermEntry(entry_set.position, file_dumper, false);
    FileOffsetOfSkipPostingBags off_skip_offs = 
      DumpTermEntry(entry_set.offset, file_dumper, false);

    return ResultOfDumpingTermEntrySetWithBloom(
        SkipListWriter(docid_skip_offs, tf_skip_offs, 
                       pos_skip_offs,   off_skip_offs, 
                       entry_set.docid.Values()),
        pos_start_offset, 
        bloom_start_offset
        );
  }

  void DumpBloom(off_t posting_list_start, FileDumper *file_dumper, 
      const BloomFilterColumnWriter &writer) 
  {
    off_t start_off = file_dumper->CurrentOffset();

    std::size_t est_skip_list_sz = EstimateBloomSkipListSize(
        start_off + 512 * 1024 - posting_list_start,
        GetSerializedOffsets(writer));

    // Dump bloom boxes
    file_dumper->Seek(start_off + est_skip_list_sz); 
    std::vector<off_t> real_box_offs = GetRelativeOffsets(
        posting_list_start,
        writer.Dump(file_dumper));

    off_t bloom_end_off = file_dumper->CurrentOffset();

    // Dump the real skip list
    BloomSkipListWriter skip_writer(real_box_offs);
    file_dumper->Seek(start_off);
    std::string data = skip_writer.Serialize();
    LOG_IF(FATAL, data.size() > est_skip_list_sz)
      << "Data corruption! Bloom skip list overwrites other stuff";
    LOG_IF(FATAL, est_skip_list_sz - data.size() > 4)
      << "Wasting too much space for bloom skip list";
    file_dumper->Dump(data);

    file_dumper->Seek(bloom_end_off);
  }

  void DumpVarint(uint32_t val) {
    VarintBuffer buf;
    buf.Append(val);
    index_dumper_.Dump(buf.Data()); 
  }

  FileDumper index_dumper_;
  FakeFileDumper fake_index_dumper_;
  bool use_bloom_filters_;

  TermIndexDumper term_index_dumper_;
  BloomFilterStore *bloom_store_begin_;
  BloomFilterStore *bloom_store_end_;
};


// Usage:
// FlashEngineDumper dumper("dump_to_dir_path")
// dumpr.LoadLocalDocuments(line doc path, ..) 
// OR dumper.LoadQqMemDump(qq mem dump path)
// dumper.Dump() it will dump to "dump_to_dir_path"
//
// Better design: decouple setting dump path and construction
class FlashEngineDumper {
 public:
  FlashEngineDumper(const std::string dump_dir_path, 
      const bool use_bloom_filters = false)
    :inverted_index_(dump_dir_path),
     dump_dir_path_(dump_dir_path),
     use_bloom_filters_(use_bloom_filters)
  {}

  // colum 2 should be tokens
  int LoadLocalDocuments(const std::string &line_doc_path, 
      int n_rows, const std::string format) {
    int ret = 0;
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

  void Dump() {
    std::cout << "Dumping inverted index...\n";
    DumpInvertedIndex();

    std::cout << "Dumping doc length...\n";
    doc_lengths_.Serialize(utils::JoinPath(dump_dir_path_, "my.doc_length"));

    std::cout << "Dumping doc store...\n";
    doc_store_.Dump(dump_dir_path_ + "/my.fdx", dump_dir_path_ + "/my.fdt");
 }

  void DumpInvertedIndex() {
    if (use_bloom_filters_ == true) {
      inverted_index_.SetBloomStore(&bloom_store_end_);
    } else {
      inverted_index_.SetBloomStore(nullptr);
    }

    inverted_index_.Dump();
  }

  // only for testing
  void SeekInvertedIndexDumper(off_t pos) {
    inverted_index_.SeekFileDumper(pos);
  }

  void DeserializeMeta(std::string path) {
    int fd;
    char *addr;
    size_t file_length;

    utils::MapFile(path, &addr, &fd, &file_length);

    next_doc_id_ = *((int *)addr);
    
    utils::UnmapFile(addr, fd, file_length);
  }

  void LoadQqMemDump(std::string dir_path) {
    std::cout << "Deserializing meta...\n";
    DeserializeMeta(dir_path + "/engine_meta.dump"); // good

    std::cout << "Deserializing doc store...\n";
    doc_store_.Deserialize(dir_path + "/doc_store.dump"); //good

    std::cout << "Deserializing inverted index...\n";
    inverted_index_.Deserialize(dir_path + "/inverted_index.dump"); //good

    std::cout << "Deserializing doc length...\n";
    doc_lengths_.Deserialize(dir_path + "/doc_lengths.dump"); //good

    std::cout << "Reset similarity...\n";
    similarity_.Reset(doc_lengths_.GetAvgLength()); //good

    if (use_bloom_filters_ == true) {
      std::cout << "Loading bloom filter..." << std::endl;
      bloom_store_end_.Deserialize(dir_path + "/bloom_filter_end.dump");
    }
  }

 private:
  // FlashDocStoreDumper doc_store_;
  ChunkedDocStoreDumper doc_store_;
  VacuumInvertedIndexDumper inverted_index_;
  DocLengthCharStore doc_lengths_;
  SimpleHighlighter highlighter_;
  Bm25Similarity similarity_;
  BloomFilterStore bloom_store_end_;

  int next_doc_id_ = 0;
  std::string dump_dir_path_;
  bool use_bloom_filters_;

  int NextDocId() {
    return next_doc_id_++;
  }
};

#endif
