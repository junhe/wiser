This python package is a wrapper of Lucence benchmark. It basically prepares 
related resources and execute `ant run-task -dALG=...`. 

You can configure the run in driver.py, at ExperimentWiki.conf(). There are
already several sets of configurations there. Enable your desired set of 
configuration by removing the `#` prefix.

# Query format

Lucene benchmark takes queries in the format of `"Hello" AND "world"`, with the quote marks.
It does take `Hello AND world`, without the quote marks. 

