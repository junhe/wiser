# This Makefile is for organizing frequently used commands, not for compilation.

hash_benchmark:
	./build.sh
	./build/hash_benchmark

grpc_benchmark:
	./build.sh
	python ./tools/run_client_server.py ./build/grpc_benchmark ./build/qq_server

in_proc_engine_bench:
	./build.sh
	./build/in_proc_engine_bench

run_qq_mem:
	./build.sh
	python ./tools/run_client_server.py ./build/client "" ./build/qq_server 50051,0

server:
	./build.sh
	./build/qq_server 50051 0

client:
	./build/client

test:
	rm -f ./build/testmain
	./build.sh
	./build/testmain

test_args:
	rm -f ./build/testmain
	./build.sh
	./build/testmain ${args}


