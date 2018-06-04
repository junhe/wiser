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



class InvertedIndexDumperBase : public InvertedIndexQqMemDelta {
 public:
  void Dump() {
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
  virtual void DumpPostingList(
      const Term &term, const PostingListDelta &posting_list) = 0;

};

struct ResultOfDumpingTermEntrySet {
  ResultOfDumpingTermEntrySet(SkipListWriter writer, off_t offset)
    :skip_list_writer(writer), pos_start_offset(offset)
  {}

  SkipListWriter skip_list_writer;
  off_t pos_start_offset;
};

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


class BloomFilterColumnWriter {
 public:
  BloomFilterColumnWriter(std::size_t array_bytes)
    : array_bytes_(array_bytes) {}

  void AddPostingBag(const std::string &bit_array) {
    bit_arrays_.push_back(bit_array);
  }

  std::vector<off_t> Dump(FileDumper *file_dumper) {
    std::vector<BloomBoxWriter> writers = GetWriters();
    std::vector<off_t> ret;

    for (auto &writer : writers) {
      ret.push_back(file_dumper->CurrentOffset());
      file_dumper->Dump(writer.Serialize());  
    }

    return ret;
  }

 private:
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

  std::size_t array_bytes_;
  std::vector<std::string> bit_arrays_;
};



class VacuumInvertedIndexDumper : public InvertedIndexDumperBase {
 public:
  VacuumInvertedIndexDumper(const std::string dump_dir_path)
    :index_dumper_(dump_dir_path + "/my.vacuum"),
     fake_index_dumper_(dump_dir_path + "/fake.vacuum"),
     term_index_dumper_(dump_dir_path + "/my.tip")
  {
  }

  // Only for testing at this time
  void SeekFileDumper(off_t pos) {
    index_dumper_.Seek(pos);
    fake_index_dumper_.Seek(pos);
  }

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


  void DumpVarint(uint32_t val) {
    VarintBuffer buf;
    buf.Append(val);
    index_dumper_.Dump(buf.Data()); 
  }

  FileDumper index_dumper_;
  FakeFileDumper fake_index_dumper_;

  TermIndexDumper term_index_dumper_;
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
  FlashEngineDumper(const std::string dump_dir_path)
    :inverted_index_(dump_dir_path),
     dump_dir_path_(dump_dir_path)
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
  }

 private:
  // FlashDocStoreDumper doc_store_;
  ChunkedDocStoreDumper doc_store_;
  VacuumInvertedIndexDumper inverted_index_;
  DocLengthCharStore doc_lengths_;
  SimpleHighlighter highlighter_;
  Bm25Similarity similarity_;

  int next_doc_id_ = 0;
  std::string dump_dir_path_;

  int NextDocId() {
    return next_doc_id_++;
  }
};

#endif
