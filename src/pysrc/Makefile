wiki:
	python -m in_mem.wiki

lucene_driver:
	python -m bench_driver.driver

test:
	python -m unittest discover -s ./ -v -p '*test*.py' 

test_esbench:
	python -m bench_driver.test_esbench

test_utils:
	python -m utils.test_utils

test_redisbench:
	python -m bench_driver.test_redisbench

start_redisearch_servers:
	python -m bench_driver.start_redis_server 5

go_rsbench:
	python -m bench_driver.go_rsbench

esbench:
	python -m bench_driver.esbench

redisearch:
	python -m bench_driver.redisearch_bench

preprocess:
	python -m in_mem.wiki preprocess

cleanup_query:
	python -m tools.cleanup_query_log

update_requirements_txt:
	pip install pipreqs
	pipreqs ./



es_health:
	curl -XGET 'localhost:9200/_cat/health?v&pretty'

es_indices:
	curl -XGET 'localhost:9200/_cat/indices?v&pretty'




