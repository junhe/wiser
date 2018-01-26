#ifndef POSTING_LIST_DELTA_H
#define POSTING_LIST_DELTA_H

class PostingListDelta {
 public:
  PostingListDelta(){}

  void AddPosting(const RankingPostingWithOffsets &posting) {
  }

 private:
  std::string data_;
  const Term term_;
  int n_postings = 0;
};

#endif
