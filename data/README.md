# README

## download query log
get-AOL-query.sh
get-wiki-query.sh

## generate synthetic log
`python generate_synthetic_log.py input_log output_file locality_parameter size'
* locality_parameter:
	* window size, where in this window, no queries are the same (generated from realistic log)
        * To note: there may be very similar queries, though
* size:
        * max number of log you want
	* Methodology: while maintaining the locality requirement, generate synthetic log from the begging of the realistic log, but when the size gets to the limit, finish generating
