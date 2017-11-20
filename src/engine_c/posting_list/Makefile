all: protobuf 

protoc_middleman: posting_message.proto
	protoc $$PROTO_PATH --cpp_out=. posting_message.proto

protobuf: main_test.cc posting_list_protobuf.cc posting_basic.cc protoc_middleman
	pkg-config --cflags protobuf  # fails if protobuf is not installed
	c++ --std=c++11 main_test.cc posting_message.pb.cc posting_list_protobuf.cc posting_basic.cc -o main_test `pkg-config --cflags --libs protobuf`

raw: raw_test.cc posting_list_raw.cc posting_basic.cc
	c++ --std=c++11 raw_test.cc posting_list_raw.cc posting_basic.cc -o raw_test

clean: 
	rm main_test posting_message.pb.cc posting_message.pb.h
