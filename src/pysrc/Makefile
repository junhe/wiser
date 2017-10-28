wiki:
	python -m in_mem.wiki

lucene_driver:
	python -m bench_driver.driver

test:
	python -m unittest discover -s ./ -v -p '*test*.py' 

test_esbench:
	python -m bench_driver.test_esbench

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


