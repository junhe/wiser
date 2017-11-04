wiki:
	python -m in_mem.wiki

lucene_bench:
	python -m benchmarks.lucene_bench

test:
	python -m unittest discover -s ./ -v -p '*test*.py' 

test_esbench:
	python -m benchmarks.test_esbench

test_utils:
	python -m utils.test_utils

test_redisbench:
	python -m benchmarks.test_redisbench

start_redisearch_servers:
	python -m benchmarks.start_redis_server 5

go_rsbench:
	python -m benchmarks.go_rsbench

esbench:
	python -m benchmarks.esbench

redisearch:
	python -m benchmarks.redisearch_bench

preprocess:
	python -m in_mem.wiki preprocess

cleanup_query:
	python -m tools.cleanup_query_log

add_date:
	python -m tools.add_date

update_requirements_txt:
	pip install pipreqs
	pipreqs ./



es_health:
	curl -XGET 'localhost:9200/_cat/health?v&pretty'

es_indices:
	curl -XGET 'localhost:9200/_cat/indices?v&pretty'




