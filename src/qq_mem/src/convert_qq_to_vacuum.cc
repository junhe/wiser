#include "qq_mem_engine.h"
#include "vacuum_engine.h"


void Convert() {
  std::string dir_path = "/mnt/ssd/vacuum_engine_dump";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine_dumper(dir_path);
  assert(engine_dumper.TermCount() == 0);
  engine_dumper.LoadQqMemDump("/mnt/ssd/big-engine-char-length-04-19");
  assert(engine_dumper.TermCount() > 1000000);

  std::cout << "Dumping to vacuum format... " << std::endl;
  engine_dumper.Dump();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 2; 

  std::cout << "9999999999y237927947293492u\n";
	gflags::ParseCommandLineFlags(&argc, &argv, true);

  Convert();
}


