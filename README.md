This repository contains code for Vacuum, a clean-slate search engine designed to exploit modern Flash-based SSDs. Vacuum utilizes many techniques to exploit flash bandwidth, including an optimized data layout, a novel two-way cost-aware Bloom filter, adaptive prefetching, and various space-time trade-offs. In experiments, we find that Vacuum increases query throughput by up to 2.7x and reduces latency by 16x when compared to the state-of-the-art Elasticsearch. For some workloads, Vacuum, with very limited memory and ample SSD bandwidth, performs better than Elasticsearch with memory that accommodates the entire working set.

# Directory structure

The main C++ code of Vaccuum is in src/qq_mem/. We also have lots of experimental code that we would like to keep in the repository, at least for now. 

- data/ Data for benchmarking and some scripts to manipulate the data.
- scripts/ A bunch of Python and Shell scripts for our experiments and setup.
- src/
    - lucene/ a copy of lucene code. We played with it.
    - pysrc/ some Python code
        - benchmarks/ scripts for benchmarking redisearch, elasticsearch, ...
        - in_mem/ we developed a minimal python in-memory engine here.
    - qq_mem/ this is the main direcotry for Vaccuum. We have the name "qq_mem" because things evolve and we are too lazy to change directory names.
        - src/ Vacuum source code (C++)
        - tools/ A bunch of helper scripts
        - README.md Instruction on how to run Vacuum
    - tutorials/ this has some Lucene examples that we played with.

# Build and run Vacuum

Please see src/qq_mem/README.md
