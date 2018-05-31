#ifndef FILE_DUMPER_H
#define FILE_DUMPER_H

class GeneralFileDumper {
 public:
  GeneralFileDumper(const std::string path) {
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666); 
    if (fd_ == -1) 
      LOG(FATAL) << "Cannot open file: " << path;
  }

  virtual off_t CurrentOffset() const {
    off_t off = lseek(fd_, 0, SEEK_CUR);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  virtual off_t Seek(off_t pos) {
    off_t off = lseek(fd_, pos, SEEK_SET);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  virtual off_t SeekToEnd() {
    off_t off = lseek(fd_, 0, SEEK_END);
    if (off == -1)
      LOG(FATAL) << "Failed to get the current offset.";

    return off;
  }

  virtual off_t End() {
    off_t old = CurrentOffset();

    off_t end_off = lseek(fd_, 0, SEEK_END);
    if (end_off == -1)
      LOG(FATAL) << "Failed to get the ending offset.";

    Seek(old);

    return end_off;
  }

  virtual off_t Dump(const std::string &data) {
    off_t start_byte = CurrentOffset();
    utils::Write(fd_, data.data(), data.size());
    return start_byte;
  }

  virtual void Flush() const {
    fsync(fd_);
  }

  virtual void Close() const {
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

  virtual FileOffsetsOfBlobs Dump(const CozyBoxWriter &writer) {
    std::vector<off_t> pack_offs = DumpPackedBlocks(writer.PackWriters());
    std::vector<off_t> vint_offs = DumpVInts(writer.VInts());

    return FileOffsetsOfBlobs(pack_offs, vint_offs);
  }

  virtual off_t Dump(const std::string &data) {
    return GeneralFileDumper::Dump(data);
  }

 protected:
  virtual std::vector<off_t> DumpVInts(const VIntsWriter &varint_buf) {
    if (varint_buf.IntsSize() == 0) {
      return std::vector<off_t>{};
    } else {
      off_t start_byte = CurrentOffset();
      std::string data_buf = varint_buf.Serialize();

      utils::Write(fd_, data_buf.data(), data_buf.size());
      return std::vector<off_t>{start_byte};
    }
  }

  virtual std::vector<off_t> DumpPackedBlocks(
      const std::vector<LittlePackedIntsWriter> &pack_writers) {
    std::vector<off_t> offs;

    for (auto &writer : pack_writers) {
      offs.push_back(DumpPackedBlock(writer));
    }

    return offs;
  }

  virtual off_t DumpPackedBlock(const LittlePackedIntsWriter &writer) {
    off_t start_byte = CurrentOffset();
    std::string data = writer.Serialize(); 

    utils::Write(fd_, data.data(), data.size());
    return start_byte;
  }
};

class FakeFileDumper : public FileDumper {
 public:
  FakeFileDumper(const std::string path) : FileDumper(path) {}

  FileOffsetsOfBlobs Dump(const CozyBoxWriter &writer) {
    std::vector<off_t> pack_offs = DumpPackedBlocks(writer.PackWriters());
    std::vector<off_t> vint_offs = DumpVInts(writer.VInts());

    return FileOffsetsOfBlobs(pack_offs, vint_offs);
  }

  off_t CurrentOffset() const {
    return cur_off_;
  }

  off_t Seek(off_t pos) {
    cur_off_ = pos;
    return pos;
  }

  off_t SeekToEnd() {
    cur_off_ = end_off_;
    return cur_off_;
  }

  off_t End() {
    return end_off_;
  }

  off_t Dump(const std::string &data) {
    off_t start_byte = CurrentOffset();

    Write(data.size());

    return start_byte;
  }

  void Flush() const {
  }

  void Close() const {
  }

 protected:
  void Write(int data_size) {
    cur_off_ += data_size;
    if (cur_off_ > end_off_)
      end_off_ = cur_off_;
  }

  std::vector<off_t> DumpVInts(const VIntsWriter &varint_buf) {
    if (varint_buf.IntsSize() == 0) {
      return std::vector<off_t>{};
    } else {
      off_t start_byte = CurrentOffset();
      std::string data_buf = varint_buf.Serialize();

      Write(data_buf.size());
      return std::vector<off_t>{start_byte};
    }
  }

  std::vector<off_t> DumpPackedBlocks(
      const std::vector<LittlePackedIntsWriter> &pack_writers) {
    std::vector<off_t> offs;

    for (auto &writer : pack_writers) {
      offs.push_back(DumpPackedBlock(writer));
    }

    return offs;
  }

  off_t DumpPackedBlock(const LittlePackedIntsWriter &writer) {
    off_t start_byte = CurrentOffset();
    std::string data = writer.Serialize();      

    Write(data.size());
    return start_byte;
  }

  off_t cur_off_ = 0;
  off_t end_off_ = 0;
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



#endif


