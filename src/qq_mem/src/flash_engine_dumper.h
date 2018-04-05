#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <iostream>

#include "qq_mem_engine.h"
#include "packed_value.h"


class PositionTermEntry {
 public:
  PositionTermEntry(PopIteratorService* pos_iter) {
    int prev_pos = 0;
    while (pos_iter->IsEnd() != true) {
      uint32_t pos = pos_iter->Pop();
      deltas_.push_back(pos - prev_pos);
      prev_pos = pos;
    }

    Fill();
  }

  std::vector<uint32_t> Deltas() const {
    return deltas_;
  }

  const VarintBuffer &VInts() const {
    return vints_;
  }

  const std::vector<PackedIntsWriter> &PackWriters() const {
    return pack_writers_;
  }

 private:
  void Fill() {
    const int pack_size = PackedIntsWriter::PACK_SIZE;
    const int n_packs = deltas_.size() / pack_size;
    const int n_remains = deltas_.size() % pack_size;

    pack_writers_.resize(n_packs);
    for (int pack_i = 0; pack_i < n_packs; pack_i++) {
      for (int offset = 0; offset < pack_size; offset++) {
        int delta_idx = pack_i * pack_size + offset;
        pack_writers_[pack_i].Add(deltas_[delta_idx]);
      }
    }
    
    for (int i = n_packs * pack_size; i < deltas_.size(); i++) {
      vints_.Append(deltas_[i]);
    }
  }

  std::vector<uint32_t> deltas_;
  std::vector<PackedIntsWriter> pack_writers_;
  VarintBuffer vints_;
};


class PositionDumper {
 public:
  PositionDumper(const std::string path) {
    fd = open(path.c_str(), O_WRONLY | O_CREAT, 0666); 
    if (fd == -1) 
      LOG(FATAL) << "Cannot open file: " << path;
  }

  void Dump(const PositionTermEntry &entry) {
    
  }

  off_t CurrentOffset() const {
    off_t off = lseek(fd, 0, SEEK_CUR);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  ~PositionDumper() {
    close(fd);
  }

 private:
  void DumpPackedBlocks(const std::vector<PackedIntsWriter> &pack_writers) {

  }

  off_t DumpPackedBlock(const PackedIntsWriter &writer) {
    off_t start_byte = CurrentOffset();
    std::string data = writer.Serialize();      
    write(fd, data.data(), data.size());
  }

  int fd;
};


class InvertedIndexDumper : public InvertedIndexQqMemDelta {
 public:
  void Dump(const std::string dir_path) {
    std::cout << "Dumping Inverted Index...." << std::endl; 
  }
};


class FlashEngineDumper {
 public:
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



