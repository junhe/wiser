# This Makefile is for organizing frequently used commands, not for compilation.

hash_benchmark:
	./build.sh
	./build/hash_benchmark

grpc_benchmark:
	./build.sh
	python ./tools/run_client_server.py ./build/grpc_benchmark ./build/qq_server

run_qq_mem:
	./build.sh
	python ./tools/run_client_server.py ./build/client ./build/qq_server

test:
	rm -f ./build/testmain
	./build.sh
	./build/testmain

