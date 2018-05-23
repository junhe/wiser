
files = """
doc_length_store.h  engine_services.h      grpc_client_impl.cc  highlighter.h       packed_value.cc       query_processing.cc   tests_13.cc  tests_5.cc  types.cc
doc_store.cc        flash_engine_dumper.h  grpc_client_impl.h   histogram.h         packed_value.h        qq_client.cc      query_processing.h    tests_14.cc  tests_6.cc  types.h
doc_store.h         flash_iterators.cc     grpc_server_impl.cc   packing_bench.cc  qq_mem_engine.cc  scoring.cc            test_helpers.cc  tests_15.cc  tests_7.cc  utils.cc
engine_bench.cc     flash_iterators.h      grpc_server_impl.h           posting.cc            qq_mem_engine.h   scoring.h             test_helpers.h   tests_16.cc  tests_8.cc  utils.h
engine_factory.h    flashreader.cc         hash_benchmark.cc    intersect_bench.cc  posting.h             qq_server.cc      simple_buffer_pool.h  tests_10.cc      tests_2.cc   tests_9.cc  vacuum_engine.cc
compression.h            engine_loader.cc    general_config.h       hash_benchmark.h     posting_list_delta.h  query_pool.cc     snippet_bench.cc      tests_11.cc      tests_3.cc   tests.cc    vacuum_engine.h
convert_qq_to_vacuum.cc  engine_loader.h     grpc_bench.cc          highlighter.cc       lrucache.h          posting_list_vec.h    query_pool.h      sorting_bench.cc      tests_12.cc      tests_4.cc
"""

for line in files.split("\n"):
    for filename in line.split():
        print filename


