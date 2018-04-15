#include "test_helpers.h"
#include "flash_iterators.h"

StandardPosting create_posting(DocIdType doc_id, 
                                         int term_freq,
                                         int n_offset_pairs) {
  return create_posting(doc_id, term_freq, n_offset_pairs, 0);
}


StandardPosting create_posting(DocIdType doc_id, 
                                         int term_freq,
                                         int n_offset_pairs,
                                         int n_positions) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < n_offset_pairs; i++) {
    offset_pairs.push_back(std::make_tuple(i * 2, i * 2 + 1)); 
  }

  Positions positions;
  for (int i = 0; i < n_positions; i++) {
    positions.push_back(i);
  }

  StandardPosting posting(doc_id, term_freq, offset_pairs, positions); 
  return posting;
}


PostingList_Vec<StandardPosting> create_posting_list_vec(int n_postings) {
  PostingList_Vec<StandardPosting> pl("hello");
  for (int i = 0; i < n_postings; i++) {
    pl.AddPosting(create_posting(i, i * 2, 5));
  }
  return pl;
}

PostingListStandardVec create_posting_list_standard(int n_postings) {
  PostingListStandardVec pl("hello");
  for (int i = 0; i < n_postings; i++) {
    pl.AddPosting(create_posting(i, i * 2, 5, 6));
  }
  return pl;
}

PostingListDelta create_posting_list_delta(int n_postings) {
  PostingListDelta pl("hello");
  for (int i = 0; i < n_postings; i++) {
    pl.AddPosting(create_posting(i, i * 2, 5, 6));
  }
  return pl;
}

FileOffsetsOfBlobs DumpCozyBox(std::vector<uint32_t> vec, 
    const std::string path, bool do_delta) {
  GeneralTermEntry entry;
  for (auto x : vec) {
    entry.AddPostingBag({x});
  }
  CozyBoxWriter writer = entry.GetCozyBoxWriter(do_delta);

  FileDumper file_dumper(path);
  FileOffsetsOfBlobs file_offs = file_dumper.Dump(writer);
  file_dumper.Close();

  return file_offs;
}



SkipList CreateSkipList(const std::string type, std::vector<off_t> offsets_of_bags) {
  SkipList skip_list; 
  
  for (int i = 0; i < offsets_of_bags.size(); i++) {
    if (type == "TF") {
      skip_list.AddEntry(10000 + i, 0, offsets_of_bags[i], 0, 0, 0); 
    } else {
      LOG(FATAL) << "Type " << type << " not supported yet";
    }
  }

  return skip_list;
}


SkipList CreateSkipListForDodId(
    std::vector<uint32_t> skip_doc_ids, std::vector<off_t> offsets_of_bags) {
  SkipList skip_list; 
  
  for (int i = 0; i < offsets_of_bags.size(); i++) {
    skip_list.AddEntry(skip_doc_ids[i], offsets_of_bags[i], 0, 0, 0, 0); 
  }

  return skip_list;
}


SkipList CreateSkipListForPosition( std::vector<off_t> blob_offsets, 
                                    std::vector<int> in_blob_indexes) 
{
  SkipList skip_list; 
  
  for (int i = 0; i < blob_offsets.size(); i++) {
    skip_list.AddEntry(0, 0, 0, blob_offsets[i], in_blob_indexes[i], 0); 
  }

  return skip_list;
}




