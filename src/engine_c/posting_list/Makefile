all: test 

protoc_middleman: posting_message.proto
	protoc $$PROTO_PATH --cpp_out=. posting_message.proto

test: main_test.cc protoc_middleman
	pkg-config --cflags protobuf  # fails if protobuf is not installed
	c++ --std=c++11 main_test.cc posting_message.pb.cc Posting_List.cc -o main_test `pkg-config --cflags --libs protobuf`

raw: raw_test.cc Posting_List_Raw.h
	c++ --std=c++11 raw_test.cc -o raw_test

clean: 
	rm main_test posting_message.pb.cc posting_message.pb.h
