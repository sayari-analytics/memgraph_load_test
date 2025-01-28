# Memgraph Load Test
Simple script to put Memgraph's trade graph under load

## Installation
```
npm install
```

## Run
The script supports the following configurations via ENV variables:
* TIME - how long to run the script before exiting. by default runs indefinitely [optional]
* HOST - memgraph trade graph host [default: localhost]
* PORT - memgraph trade graph port [default: 7687]
* CONCURRENCY - max number of concurrent queries [default: 18]
* TIMEOUT - query timeout in ms. by default, no timeout is set [optional]
* MIN_SUPPLY_CHAIN_SIZE - minimum size of supply chain. raising this number restricts queries to more expensive nodes [default: 2000]
* QUERY_RESPONSE_TYPE - pass 'graph' to return the entire supply chain graph. pass 'count' to return only supply chain count [default: graph]
* USING_HOPS_LIMIT - limit the number of edges traversed using Memgraph's `USING HOPS LIMIT` directive. by default, directive is not included [optional]
* QUERY_MEMORY_LIMIT - limit the query memory usage using Memgraph's `QUERY MEMORY LIMIT` directive. by default, no limit is set [optional]

To run
```
HOST=some.service PORT=7688 npx tsx index.ts
```
