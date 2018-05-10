# This Makefile is for organizing frequently used commands, not for compilation.
.ONESHELL:

NUM_CORES=8

all:
	./build.sh 2>&1 | tee build.log

hash_benchmark:
	./build.sh
	./build/hash_benchmark

scoring_benchmark:
	rm -f ./build/scoring_bench
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j${NUM_CORES} scoring_bench
	
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
	sudo pkill qq_server
	sudo pkill engine_bench
	python ./tools/run_client_server2.py \
		./build/engine_bench '-exp_mode=grpclog -n_threads=16 -use_profiler=true -grpc_server=localhost' \
		./build/qq_server '-sync_type=ASYNC -n_threads=1 -addr=localhost -engine=vacuum:vacuum_dump:/mnt/ssd/vacuum_engine_dump_magic' \
		20	
		#./build/engine_bench '-exp_mode=grpclog -n_threads=16 -use_profiler=true -grpc_server=localhost:50051' \

engine_bench:
	cd build && cmake ../src/ && \
		make -j${NUM_CORES} engine_bench && \
		CPUPROFILE_FREQUENCY=1000 ./engine_bench -exp_mode=locallog -use_profiler=true
		#HEAPPROFILE=/tmp/heapprof ./engine_bench -exp_mode=locallog -use_profiler=true

packing_bench:
	cd build && cmake ../src/ && \
		make -j${NUM_CORES} packing_bench && \
		./packing_bench

heap_check:
	cd build && cmake ../src/ && \
		make -j${NUM_CORES} engine_bench && \
		HEAPCHECK=normal ./engine_bench -exp_mode=locallog -use_profiler=false



engine_bench_build:
	cd build && cmake ../src/ && make -j${NUM_CORES} engine_bench &&
	make -j${NUM_CORES} qq_server

engine_bench_debug:
	cd build && cmake ../src/ && \
		make -j${NUM_CORES} engine_bench && \
		gdb --args ./engine_bench -exp_mode=locallog -use_profiler=false

convert_qq_to_vacuum:
	cd build && cmake ../src/ && \
		make -j${NUM_CORES} convert_qq_to_vacuum && \
		./convert_qq_to_vacuum


vacuum_two_nodes:
	cd build 
	cmake ../src/
	make -j${NUM_CORES} qq_server 
	make -j${NUM_CORES} engine_bench
	cd ..
	python ./tools/run_vacuum_two_nodes.py

elastic_two_nodes:
	python ./tools/run_elastic_two_nodes.py

exp:
	python ./tools/run_exp.py

profile:
	google-pprof --text ./build/engine_bench ./build/my.profile.new

server_profile:
	google-pprof --text ./build/qq_server ./vacuum.profile

server_profile_gv:
	google-pprof --pdf ./build/qq_server ./vacuum.profile > profile.pdf

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
	make -j${NUM_CORES} testmain
	cd ..
	./build/testmain ${args}

test_fast:
	rm -f ./build/testmain
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j${NUM_CORES} testmain
	cd ..
	./build/testmain '~[grpc]~[slow]'

test_args:
	rm -f ./build/testmain
	mkdir -p ./build
	cd build
	cmake ../src/
	make -j${NUM_CORES} testmain
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
