project(grpc-cmake-example)
cmake_minimum_required(VERSION 3.2)

add_compile_options(-std=c++11 -O3)

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




protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_SRC_DIR} ${PROTOS})
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${PROTO_SRC_DIR} ${PROTOS})


set(ENGINE_SRCS 
    native_doc_store.cc
    posting_basic.cc
    posting_list_direct.cc
    posting_list_protobuf.cc
    posting_list_raw.cc
    inverted_index.cc
    index_creator.cc
    qq_engine.cc
    grpc_client_impl.cc
    utils.cc
    intersect.cc
    ranking.cc
    unifiedhighlighter.cc
    )

set(BOOST_LIBS
    boost_system
    boost_filesystem
    boost_locale
    )

set(GLOG_LIB glog)


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
    gRPC::grpc++_reflection
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
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)



#######################################
# Catch2 testing framework
#######################################
add_library( CatchMain OBJECT catch_main.cc catch.hpp )
add_executable( testmain tests.cc 
    grpc_client_impl.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
    $<TARGET_OBJECTS:CatchMain> catch.hpp )
target_link_libraries(testmain
    gRPC::grpc++_reflection
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

add_executable(in_proc_engine_bench
    in_proc_engine_bench.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(in_proc_engine_bench
    gRPC::grpc++_reflection
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
    gRPC::grpc++_reflection
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
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)

add_executable(ranking_bench
    ranking_bench.cc
    ${ENGINE_SRCS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_link_libraries(ranking_bench
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    ${BOOST_LIBS}
    pthread
    ${GLOG_LIB}
)


