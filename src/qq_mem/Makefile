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
		./build/in_proc_engine_bench_querylog '-use_grpc=true -n_threads=16 -use_profiler=false' \
		./build/qq_server '-sync_type=ASYNC -n_threads=1' \
		30

in_proc_engine_bench_querylog:
	cd build && cmake ../src/ && \
		make -j32 in_proc_engine_bench_querylog && \
		CPUPROFILE_FREQUENCY=1000 ./in_proc_engine_bench_querylog -exp_mode=local -use_profiler=true

profile:
	google-pprof --text ./build/in_proc_engine_bench_querylog ./build/my.profile.new

profile_gv:
	google-pprof --pdf ./build/in_proc_engine_bench_querylog ./build/my.profile.new > profile.pdf

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

snippet_bench:
	./build.sh
	./build/snippet_bench
