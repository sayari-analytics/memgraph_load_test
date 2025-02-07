import * as fs from 'fs'
import neo4j, { Integer } from 'neo4j-driver'

const HOST = process.env.HOST ?? 'localhost'
const PORT = process.env.PORT ?? '7687'
const TIME = process.env.TIME !== undefined ? parseInt(process.env.TIME, 10) : undefined
const CONCURRENCY = process.env.CONCURRENCY !== undefined ? parseInt(process.env.CONCURRENCY, 10) : 18
const TIMEOUT = process.env.TIMEOUT !== undefined ? parseInt(process.env.TIMEOUT) : undefined
const MIN_SUPPLY_CHAIN_SIZE = process.env.MIN_SUPPLY_CHAIN_SIZE !== undefined ? parseInt(process.env.MIN_SUPPLY_CHAIN_SIZE, 10) : 2000
const QUERY_DATE_FILTER = process.env.QUERY_DATE_FILTER ?? 'path_date_filter'
const QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL = process.env.QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL !== undefined
const QUERY_RESPONSE_TYPE = process.env.QUERY_RESPONSE_TYPE ?? 'graph'
const QUERY_HOPS_LIMIT = process.env.QUERY_HOPS_LIMIT !== undefined ? parseInt(process.env.QUERY_HOPS_LIMIT, 10) : undefined
const QUERY_MEMORY_LIMIT = process.env.QUERY_MEMORY_LIMIT

if (QUERY_DATE_FILTER !== 'path_date_filter' && QUERY_DATE_FILTER !== 'segment_date_filter') {
  console.error(`Invalid QUERY_DATE_FILTER param ${QUERY_DATE_FILTER}. Expected values 'path_date_filter', 'segment_date_filter'.`)
  process.exit(1)
}

if (QUERY_RESPONSE_TYPE !== 'graph' && QUERY_RESPONSE_TYPE !== 'count') {
  console.error(`Invalid QUERY_RESPONSE_TYPE param ${QUERY_RESPONSE_TYPE}. Expected values 'graph', 'map'.`)
  process.exit(1)
}

const driver = neo4j.driver(`bolt://${HOST}:${PORT}`)

const entities: Array<[string, number]> = fs.readFileSync(`${__dirname}/data.csv`, 'utf-8')
  .split('\n')
  .map((line: string) => {
    const [id, count] = line.split(',')
    return [id, parseInt(count, 10)]
  })
  .filter(([_, count]: [string, number]) => count > MIN_SUPPLY_CHAIN_SIZE)
  .sort((a: [string], b: [string]) => b[0].localeCompare(a[0]))

let entityIdx = 0
let totalRequestCount = 0
let successCount = 0
let errorCount = 0
let totalEntityCount = 0
const START_TIME = Date.now()
let QUERY = QUERY_HOPS_LIMIT ? `USING HOPS LIMIT ${QUERY_HOPS_LIMIT} ` : ''

if (QUERY_DATE_FILTER === 'path_date_filter') {
  QUERY += `
    MATCH (a:company { id: $id })<-[r1:ships_to]-(b)
      WHERE b != a
    WITH a, b, r1.hs_code AS component, r1.shipment_departure AS shipment_departure, r1.max_date AS max_date, r1.min_date AS min_date, r1
    OPTIONAL MATCH (b)<-[r2:ships_to]-(c)
      WHERE c != b AND c != a
        AND product_map.is_component(component, r2.hs_code)
        AND max_date >= r2.min_date
        AND min_date <= r2.max_date
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN shipment_departure WHERE any(arrival IN r2.shipment_arrival WHERE departure = arrival))` : ''}
    WITH a, b, c, r2.hs_code AS component, r2.shipment_departure AS shipment_departure,
      CASE
        WHEN max_date < r2.max_date THEN max_date ELSE r2.max_date
      END AS max_date,
      CASE 
        WHEN min_date > r2.min_date THEN min_date ELSE r2.min_date 
      END AS min_date,
      r1, r2
    OPTIONAL MATCH (c)<-[r3:ships_to]-(d)
      WHERE d != c AND d != b AND d != a
        AND product_map.is_component(component, r3.hs_code)
        AND max_date >= r3.min_date
        AND min_date <= r3.max_date
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN shipment_departure WHERE any(arrival IN r3.shipment_arrival WHERE departure = arrival))` : ''}
    WITH a, b, c, d, r3.hs_code AS component, r3.shipment_departure AS shipment_departure,
      CASE
        WHEN max_date < r3.max_date THEN max_date ELSE r3.max_date
      END AS max_date,
      CASE 
        WHEN min_date > r3.min_date THEN min_date ELSE r3.min_date 
      END AS min_date,
      r1, r2, r3
    WITH a, b, c, d, component, shipment_departure, max_date, min_date, collect(r1) + collect(r2) + collect(r3) AS edges
    OPTIONAL MATCH (d)<-[r4:ships_to]-(e)
      WHERE e != d AND e != c AND e != b AND e != a
        AND product_map.is_component(component, r4.hs_code)
        AND max_date >= r4.min_date
        AND min_date <= r4.max_date
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN shipment_departure WHERE any(arrival IN r4.shipment_arrival WHERE departure = arrival))` : ''}
    UNWIND edges + [r4, a, b, c, d, e] AS item
  `
} else {
  QUERY += `
    MATCH (a:company { id: $id })<-[r1:ships_to]-(b)
      WHERE b != a 
    WITH a, b, collect(r1) AS r1
    OPTIONAL MATCH (b)<-[r2:ships_to]-(c)
      WHERE c != b AND c != a AND any(export IN r1
      WHERE (
        export.min_date >= r2.min_date AND export.min_date <= r2.max_date
        AND product_map.is_component(export.hs_code, r2.hs_code)
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN export.shipment_departure WHERE any(arrival IN r2.shipment_arrival WHERE departure = arrival))` : ''}
      ))
    WITH a, b, c, r1, collect(r2) AS r2
    OPTIONAL MATCH (c)<-[r3:ships_to]-(d)
      WHERE d != c AND d != b AND d != a AND any(export IN r2
      WHERE (
        export.min_date >= r3.min_date AND export.min_date <= r3.max_date
        AND product_map.is_component(export.hs_code, r3.hs_code)
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN export.shipment_departure WHERE any(arrival IN r3.shipment_arrival WHERE departure = arrival))` : ''}
      ))
    WITH a, b, c, d, r1, r2, collect(r3) AS r3
    OPTIONAL MATCH (d)<-[r4:ships_to]-(e)
      WHERE e != d AND e != c AND e != b AND e != a AND any(export IN r3
      WHERE (
        export.min_date >= r4.min_date AND export.min_date <= r4.max_date
        AND product_map.is_component(export.hs_code, r4.hs_code)
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN export.shipment_departure WHERE any(arrival IN r4.shipment_arrival WHERE departure = arrival))` : ''}
      ))
    WITH a, b, c, d, e, r1, r2, r3, collect(r4) AS r4
    UNWIND [a, b, c, d, e] + r1 + r2 + r3 + r4 AS item
  `
}

if (QUERY_RESPONSE_TYPE === 'graph') {
  QUERY += `
    WITH item WHERE item IS NOT NULL
    RETURN extract(
      item IN collect(distinct item) |
      CASE valueType(item)
        WHEN 'NODE' THEN [ID(item), item.id, item.label, item.risk]
        ELSE [ID(startNode(item)), ID(endNode(item)), item.hs_code_int, item.max_date, item.min_date, item.product_origin, item.shipment_arrival, item.shipment_departure]
      END
    ) AS nodes_and_edges
  `
} else {
  QUERY += `
    WITH item WHERE item IS NOT NULL AND valueType(item) = 'NODE'
    WITH collect(distinct item) AS items
    RETURN size(items) AS node_count
  `
}

if (QUERY_MEMORY_LIMIT) {
  QUERY += `QUERY MEMORY LIMIT ${QUERY_MEMORY_LIMIT};`
}



const queryRunner = async () => {
  while (true) {
    const [id] = entities[entityIdx]
    entityIdx = (entityIdx + 1) % entities.length
    const session = driver.session()
    const t1 = Date.now()

    try {
      let entityCount: number
      if (QUERY_RESPONSE_TYPE === 'graph') {
        const result = await session.run<{ nodes_and_edges: ([Integer, string, string, Integer[]] | [Integer, Integer, Integer, Date, Date, Integer, Integer, Integer])[] }>(QUERY, { id }, TIMEOUT ? { timeout: TIMEOUT } : {})

        entityCount = result.records[0].get('nodes_and_edges').filter((node_or_edge) => {
          return node_or_edge.length === 4
        }).length
      } else {
        const result = await session.run<{ node_count: number }>(QUERY, { id }, TIMEOUT ? { timeout: TIMEOUT } : {})
        entityCount = Number(result.records[0].get('node_count'))
      }

      const totalResponseTime = Date.now() - START_TIME
      totalRequestCount++
      successCount++
      totalEntityCount += entityCount
  
      console.log(
        `Success. Time ${Date.now() - t1}ms. Entity count ${entityCount} ` +
        `(TOTAL: ${totalRequestCount} reqs ${Math.round(totalResponseTime / 1000)} sec) (AVG: latency ${Math.round(totalResponseTime / totalRequestCount)}ms, throughout ${Math.round((successCount / (totalResponseTime / 60000)) * 100) / 100} req/min, entities ${Math.round(totalEntityCount / successCount)}) ` +
        `[Success ${Math.round(successCount/totalRequestCount * 100)}% (${successCount}/${totalRequestCount}) Error ${Math.round(errorCount/totalRequestCount * 100)}% (${errorCount}/${totalRequestCount})] ${id} `
      )
    } catch (err) {
      const totalResponseTime = Date.now() - START_TIME
      totalRequestCount++
      errorCount++
  
      console.error(
        `Error. Time ${Date.now() - t1}ms ` +
        `(TOTAL: ${totalRequestCount} reqs ${Math.round(totalResponseTime / 1000)} sec) (AVG: latency ${Math.round(totalResponseTime / totalRequestCount)}ms, throughout ${Math.round((successCount / (totalResponseTime / 60000)) * 100) / 100} req/min) ` +
        `[Success ${Math.round(successCount/totalRequestCount * 100)}% (${successCount}/${totalRequestCount}) Error ${Math.round(errorCount/totalRequestCount * 100)}% (${errorCount}/${totalRequestCount})] ${id} ` +
        err
      )
    } finally {
      await session.close()
      if (TIME !== undefined && Date.now() - START_TIME >= TIME * 1000) {
        break
      }
    }
  }
}

console.log(`Querying Memgraph at ${HOST}:${PORT} with concurrency ${CONCURRENCY}, timeout ${TIMEOUT ?? '-'}, min supply chain size ${MIN_SUPPLY_CHAIN_SIZE}, response type ${QUERY_RESPONSE_TYPE}, hops limit ${QUERY_HOPS_LIMIT ?? '-'}, downstream_departure=upstream_arrival ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL}`)

Promise.all(
  Array(CONCURRENCY).fill(null).map(queryRunner)
)
  .then(() => driver.close())
  .catch((err) => console.error(err))
