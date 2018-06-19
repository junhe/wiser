#include "qq_mem_engine.h"
#include "flash_engine_dumper.h"


DEFINE_string(qqdump_dir_path, "", "path of the qq dump dir to read from");
DEFINE_string(vacuum_dir_path, "", "path of the vacuum to write to");


void CheckArgs() {
  if (FLAGS_qqdump_dir_path == "") {
    LOG(FATAL) << "Arg qqdump_dir_path is not set";
  }

  if (FLAGS_vacuum_dir_path == "") {
    LOG(FATAL) << "Arg vaccum_dir_path is not set";
  }
}


void Convert() {
  std::string vacuum_to_path = FLAGS_vacuum_dir_path;
  utils::PrepareDir(vacuum_to_path);

  FlashEngineDumper engine_dumper(vacuum_to_path, true);
  assert(engine_dumper.TermCount() == 0);

  engine_dumper.LoadQqMemDump(FLAGS_qqdump_dir_path);
  assert(engine_dumper.TermCount() > 1000000);

  std::cout << "Dumping to vacuum format... " << std::endl;
  engine_dumper.Dump();
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 2; 

  std::cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
	gflags::ParseCommandLineFlags(&argc, &argv, true);

  CheckArgs();

  Convert();
}


