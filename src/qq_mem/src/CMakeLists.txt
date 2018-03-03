project(grpc-cmake-example)
cmake_minimum_required(VERSION 3.2)

#add_compile_options(-std=c++11 -O3)
add_compile_options(-std=c++11 -O3 -g -fno-omit-frame-pointer)
#add_compile_options(-std=c++11 -O0 -g)
set(GPROFILER profiler)

# GRPC and Protocol Buffers libraries location
list(APPEND CMAKE_PREFIX_PATH "/opt/grpc" "/opt/protobuf")

# Cmake find modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

find_package(Protobuf REQUIRED)
find_package(GRPC REQUIRED)
find_package( Boost 1.5.4 COMPONENTS program_options REQUIRED )

set(PROTOS
    ${CMAKE_CURRENT_SOURCE_DIR}/protos/qq.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/protos/posting_message.proto
)

set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto-src)
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})
include_directories(${PROTO_SRC_DIR})
include_directories(${Boost_INCLUDE_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})



protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_SRC_DIR} ${PROTOS})
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${PROTO_SRC_DIR} ${PROTOS})


set(ENGINE_SRCS 
    doc_store.cc
    posting.cc
    query_processing.cc
    query_pool.cc
    types.cc
    qq_mem_engine.cc
    grpc_client_impl.cc
    grpc_server_impl.cc
    utils.cc
    engine_loader.cc
    scoring.cc
    highlighter.cc
    flashreader.cc
    )

set(BOOST_LIBS
    boost_system
    boost_filesystem
    boost_locale
    )

set(GRPC_LIBS
    gpr
    gRPC::grpc++_reflection
    )


set(GLOG_LIB glog ${GPROFILER})


#######################################
# GRPC Client and server
#######################################
add_executable(qq_server
    qq_server.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(qq_server
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

# Building client
add_executable(qq_client
    ${CMAKE_CURRENT_SOURCE_DIR}/qq_client.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(qq_client
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)



#######################################
# Catch2 testing framework
#######################################
add_library( CatchMain OBJECT catch_main.cc catch.hpp )
add_executable( testmain 
    tests.cc 
    tests_2.cc
    tests_3.cc
    tests_4.cc
    tests_5.cc
    tests_6.cc
    tests_7.cc
    tests_8.cc
    test_helpers.cc
    grpc_client_impl.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
    $<TARGET_OBJECTS:CatchMain> catch.hpp )
target_link_libraries(testmain
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    ${GLOG_LIB}
)


#######################################
# For benchmarks
#######################################
add_executable(hash_benchmark
    hash_benchmark.cc
    utils.cc
)

target_link_libraries(hash_benchmark
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    ${GLOG_LIB}
)



add_executable(in_proc_engine_bench_querylog
    in_proc_engine_bench_querylog.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

target_link_libraries(in_proc_engine_bench_querylog
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

add_executable(grpc_benchmark
    grpc_client_impl.cc
    ${ENGINE_SRCS}
    grpc_bench.cc
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(grpc_benchmark
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

add_executable(intersect_bench
    intersect_bench.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(intersect_bench
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

add_executable(scoring_bench
    scoring_bench.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(scoring_bench
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

add_executable(sorting_bench
    sorting_bench.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(sorting_bench
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

add_executable(snippet_bench
    snippet_bench.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(snippet_bench
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

add_executable(compression_bench
    compression_bench.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(compression_bench
    ${GRPC_LIBS}
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)


