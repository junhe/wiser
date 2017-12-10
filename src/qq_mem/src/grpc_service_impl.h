#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "qq.grpc.pb.h"
#include "qq_engine.h"
#include "inverted_index.h"
#include "native_doc_store.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

// for AddDocument
using qq::AddDocumentRequest;
using qq::StatusReply;

// for Search
using qq::SearchRequest;
using qq::SearchReply;

// for Echo
using qq::EchoData;

using qq::QQEngine;



// Logic and data behind the server's behavior.

