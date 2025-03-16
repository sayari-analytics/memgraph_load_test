# Memgraph Load Test
Simple script to put Memgraph's trade graph under load

## Installation
```
npm install
```

## Run
The script supports the following configurations via ENV variables:
* COUNT - how many queries to run before exiting. by default runs indefinitely [optional]
* HOST - memgraph trade graph host [default: localhost]
* PORT - memgraph trade graph port [default: 7687]
* CONCURRENCY - max number of concurrent queries [default: 18]
* MIN_SUPPLY_CHAIN_SIZE - minimum size of supply chain. raising this number restricts queries to more expensive nodes [default: 2000]
* QUERY_DATE_FILTER - pass `path_date_filter` to filter dates by path. pass `segment_date_filter` to filter dates by pairwise segments [default: `path_date_filter`]
* QUERY_RESPONSE_TYPE - pass `graph` to return the entire supply chain graph. pass `count` to return only supply chain count [default: `graph`]
* QUERY_HOPS_LIMIT - limit the number of edges traversed using Memgraph's `USING HOPS LIMIT` directive. by default, directive is not included [optional]
* QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL - whether to only return supply chains where the downstream departure country matches the upstream arrival country. by default false [optional]

To run
```
HOST=some.service PORT=7688 npx tsx index.ts
```
