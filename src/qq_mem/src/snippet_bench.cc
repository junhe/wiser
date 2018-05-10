#include <vector>

#include <glog/logging.h>

#include "types.h"
#include "utils.h"
#include "highlighter.h"
#include <boost/locale.hpp>


struct DocumentContext {
  int doc_id;
  std::string body;
  OffsetPairs hello_pairs;
};



Snippets highlight_top_k(std::vector<DocumentContext> & contexts, SimpleHighlighter & highlighter) {
  Snippets snippets;
  for (std::size_t i = 0; i < contexts.size(); i++) {
    auto ctx = contexts[i];
    OffsetsEnums enums = {};
    enums.push_back(Offset_Iterator(ctx.hello_pairs));
    //std::cout << highlighter.highlightOffsetsEnums(enums, 5, ctx.body) << std::endl;
    snippets.push_back(highlighter.highlightOffsetsEnums(enums, 5, ctx.body));
  }

  // std::cout << snippets.size() << std::endl;
  return snippets;
}

void bench() {
	std::vector<std::string> bodies{
		"hello l.o",
    "hello film.",
    "golden hello .",
    "hei hello .",
    "websit hello project.",
    "websit hello project.",
    "websit hello project.",
    "websit hello project.",
    "hello from mar.",
    "hello caesar german:halloh."};

  std::vector<DocumentContext> contexts;

  for (std::size_t i = 0; i < bodies.size(); i++) {
    auto body = bodies[i];
    auto doc_id = i;

    std::map<Term, OffsetPairs> term_offsets = utils::extract_offset_pairs(body);

    DocumentContext ctx;
    ctx.doc_id = doc_id;
    ctx.body = body;
    ctx.hello_pairs = term_offsets["hello"];

    contexts.push_back(ctx);
  }

  const int repeats = 100000;
  SimpleHighlighter highlighter;
  auto start = utils::now();
  for (int i = 0; i < repeats; i++) {
    highlight_top_k(contexts, highlighter);
  }
  auto end = utils::now();
  auto duration = utils::duration(start, end);
  
  std::cout << "Total duration: " << duration << std::endl;
  std::cout << "Total repeats: " << repeats << std::endl;
  std::cout << "Duration: " << duration / repeats << std::endl;
  std::cout << "Query (TopK Snippets) Per Sec: " << repeats / duration << std::endl;
}


int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 
  bench();
}


