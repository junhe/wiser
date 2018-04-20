# This Makefile is for organizing frequently used commands, not for compilation.
.ONESHELL:

all:
	./build.sh

hash_benchmark:
	./build.sh
	./build/hash_benchmark

scoring_benchmark:
	rm -f ./build/scoring_bench
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j32 scoring_bench
	
	cd ..
	./build/scoring_bench

intersect_benchmark:
	./build.sh
	./build/intersect_bench

compression_bench:
	./build.sh
	./build/compression_bench

grpc_benchmark:
	./build.sh
	echo ------------------------------------------------------
	python ./tools/run_client_server2.py \
		./build/engine_bench '-exp_mode=grpclog -n_threads=16 -use_profiler=false' \
		./build/qq_server '-sync_type=ASYNC -n_threads=1' \
		5

engine_bench:
	cd build && cmake ../src/ && \
		make -j32 engine_bench && \
		CPUPROFILE_FREQUENCY=1000 ./engine_bench -exp_mode=local -use_profiler=true

convert_qq_to_vacuum:
	cd build && cmake ../src/ && \
		make -j32 convert_qq_to_vacuum && \
		./convert_qq_to_vacuum

profile:
	google-pprof --text ./build/engine_bench ./build/my.profile.new

profile_gv:
	google-pprof --pdf ./build/engine_bench ./build/my.profile.new > profile.pdf

run_qq_mem:
	./build.sh
	python ./tools/run_client_server.py ./build/client "" ./build/qq_server 50051,0

server:
	./build.sh
	./build/qq_server 50051 0

client:
	./build/qq_client

test:
	rm -f ./build/testmain
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j32 testmain
	cd ..
	./build/testmain ${args}

test_fast:
	rm -f ./build/testmain
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j32 testmain
	cd ..
	./build/testmain '~[grpc]~[slow]'

test_args:
	rm -f ./build/testmain
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j32 testmain
	cd ..
	./build/testmain ${args}

test_qqflash:
	rm -f ./build/testmain
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j16 testmain
	cd ..
	./build/testmain "[qqflash]"

snippet_bench:
	./build.sh
	./build/snippet_bench
