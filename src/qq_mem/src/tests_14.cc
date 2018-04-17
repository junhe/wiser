#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "vacuum_engine.h"
#include "utils.h"


TEST_CASE( "Dumping 1-word Engine", "[qqflash][dump0]" ) {
  std::string dir_path = "/tmp/1-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine(dir_path);
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/one_word_with_position", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine.TermCount() == 1);

  // Dump the engine
  engine.DumpInvertedIndex();

  SECTION("Load inverted index") {
    VacuumInvertedIndex index(
        "/tmp/1-word-engine/my.tip", "/tmp/1-word-engine/my.vaccum");
    REQUIRE(index.FindPostingListOffset("a") == 0);
    REQUIRE(index.FindPostingListOffset("b") == -1);
    REQUIRE(index.NumTerms() == 1);

    SECTION("Get the posting list iterator, non-exist") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"b"});
      REQUIRE(iters.size() == 0);
    }

    SECTION("Get the posting list iterator") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"a"});
      REQUIRE(iters.size() == 1);

      VacuumPostingListIterator it = iters[0];
      REQUIRE(it.Size() == 1);
      REQUIRE(it.DocId() == 0);
      REQUIRE(it.IsEnd() == false);
      it.Advance();
      REQUIRE(it.IsEnd() == true);
    }
  }
}


TEST_CASE( "Dumping 3-word Engine", "[qqflash][dump3]" ) {
  std::string dir_path = "/tmp/3-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine(dir_path);
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/iter_test_3_docs", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine.TermCount() == 3);

  // Dump the engine
  engine.DumpInvertedIndex();

  VacuumInvertedIndex index(
      "/tmp/3-word-engine/my.tip", "/tmp/3-word-engine/my.vaccum");
  REQUIRE(index.NumTerms() == 3);

  SECTION("To iteratet doc ids, TFs, positions") {
    SECTION("Iterate doc IDs of 'a'") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"a"});
      REQUIRE(iters.size() == 1);

      VacuumPostingListIterator &it = iters[0];
      REQUIRE(it.Size() == 3);

      std::cout << "---------fffffffffffffffffffffffffffffffffffffffffffff\n";
      
      for (int i = 0; i < it.Size(); i++) {
        std::cout << "iiiiiiiiiiiiiiiii: " << i << std::endl;
        REQUIRE(it.IsEnd() == false);
        REQUIRE(it.DocId() == i);
        REQUIRE(it.TermFreq() == 1);
        REQUIRE(it.PostingIndex() == i);

        InBagPositionIterator pos_iter;
        it.AssignPositionBegin(&pos_iter);

        REQUIRE(pos_iter.Pop() == 0); 
        REQUIRE(pos_iter.IsEnd() == true); 

        it.Advance();
      }
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("Iterate doc IDs of 'b'") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"b"});
      REQUIRE(iters.size() == 1);

      VacuumPostingListIterator &it = iters[0];
      REQUIRE(it.Size() == 2);
      
      std::vector<int> doc_ids{1, 2};

      for (int i = 0; i < it.Size(); i++) {
        REQUIRE(it.IsEnd() == false);
        REQUIRE(it.DocId() == doc_ids[i]);
        REQUIRE(it.TermFreq() == 1);
        REQUIRE(it.PostingIndex() == i);

        InBagPositionIterator pos_iter;
        it.AssignPositionBegin(&pos_iter);
        REQUIRE(pos_iter.Pop() == 1); 
        REQUIRE(pos_iter.IsEnd() == true); 

        it.Advance();
      }
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("Iterate doc IDs of 'c'") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"c"});
      REQUIRE(iters.size() == 1);

      VacuumPostingListIterator &it = iters[0];
      REQUIRE(it.Size() == 1);
      
      std::vector<int> doc_ids{2};

      for (int i = 0; i < it.Size(); i++) {
        REQUIRE(it.IsEnd() == false);
        REQUIRE(it.DocId() == doc_ids[i]);
        REQUIRE(it.TermFreq() == 1);
        REQUIRE(it.PostingIndex() == i);

        InBagPositionIterator pos_iter;
        it.AssignPositionBegin(&pos_iter);
        REQUIRE(pos_iter.Pop() == 2); 
        REQUIRE(pos_iter.IsEnd() == true); 

        it.Advance();
      }
      REQUIRE(it.IsEnd() == true);
    }
  }
}


TEST_CASE( "3 word engine with different tfs", "[qqflash][tf]" ) {
  std::string dir_path = "/tmp/3-word-engine-tf";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine(dir_path);
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/iter_test_3_docs_tf", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine.TermCount() == 3);

  // Dump the engine
  engine.DumpInvertedIndex();

  VacuumInvertedIndex index(
      dir_path + "/my.tip", dir_path + "/my.vaccum");
  REQUIRE(index.NumTerms() == 3);

  SECTION("To iteratet doc ids, TFs") {
    SECTION("Iterate doc IDs of 'a'") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"a"});
      REQUIRE(iters.size() == 1);

      VacuumPostingListIterator &it = iters[0];
      REQUIRE(it.Size() == 3);
      
      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.DocId() == 0);
      REQUIRE(it.TermFreq() == 1);
      REQUIRE(it.PostingIndex() == 0);
      it.Advance();

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.DocId() == 1);
      REQUIRE(it.TermFreq() == 2);
      REQUIRE(it.PostingIndex() == 1);

      // iter positions of the second document
      InBagPositionIterator pos_iter;
      it.AssignPositionBegin(&pos_iter);
      REQUIRE(pos_iter.IsEnd() == false); 
      REQUIRE(pos_iter.Pop() == 0); 
      REQUIRE(pos_iter.IsEnd() == false); 
      REQUIRE(pos_iter.Pop() == 1); 
      REQUIRE(pos_iter.IsEnd() == true); 

      it.Advance();

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.DocId() == 2);
      REQUIRE(it.TermFreq() == 1);
      REQUIRE(it.PostingIndex() == 2);
      it.Advance();

      REQUIRE(it.IsEnd() == true);
    }
  }
}


TEST_CASE( "Dumping 5-word Engine", "[qqflash][dump1]" ) {
  std::string dir_path = "/tmp/5-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine(dir_path);
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");

  REQUIRE(engine.TermCount() > 0);
  engine.DumpInvertedIndex();

  SECTION("Load inverted index") {
    VacuumInvertedIndex index(
        "/tmp/5-word-engine/my.tip", "/tmp/5-word-engine/my.vaccum");
    REQUIRE(index.NumTerms() > 100);
  }
}


