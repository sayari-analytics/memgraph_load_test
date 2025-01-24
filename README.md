# Memgraph Load Test
Simple script to put Memgraph's trade graph under load

## Installation
```
npm install
```

## Run
The script supports the following configurations via ENV variables:
* HOST - memgraph trade graph host [default: localhost]
* PORT - memgraph trade graph port [default: 7687]
* CONCURRENCY - max number of concurrent queries [default: 18]
* TIMEOUT - query timeout in ms [default: 15000]
* MIN_SUPPLY_CHAIN_SIZE - minimum size of supply chain. raising this number restricts queries to more expensive nodes [default: 5000]

To run
```
HOST=some.service PORT=7688 npx tsx index.ts
```
