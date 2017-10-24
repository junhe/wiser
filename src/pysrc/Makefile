wiki:
	python -m in_mem.wiki

test:
	python -m unittest discover -s ./ -v -p '*test*.py' 

preprocess:
	python -m in_mem.wiki preprocess

cleanup_query:
	python -m tools.cleanup_query_log

update_requirements_txt:
	pip install pipreqs
	pipreqs ./


