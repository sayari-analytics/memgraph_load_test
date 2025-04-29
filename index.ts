import * as fs from 'fs'
import neo4j, { Integer } from 'neo4j-driver'

const HOST = process.env.HOST ?? 'localhost'
const PORT = process.env.PORT ?? '7687'
const COUNT = process.env.COUNT !== undefined ? parseInt(process.env.COUNT, 10) : Infinity
const CONCURRENCY = process.env.CONCURRENCY !== undefined ? parseInt(process.env.CONCURRENCY, 10) : 18
const MIN_SUPPLY_CHAIN_SIZE = process.env.MIN_SUPPLY_CHAIN_SIZE !== undefined ? parseInt(process.env.MIN_SUPPLY_CHAIN_SIZE, 10) : 2000
const QUERY_DATE_FILTER = process.env.QUERY_DATE_FILTER ?? 'path_date_filter'
const QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL = (process.env.QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ?? '').toLowerCase() !== 'false'
const QUERY_RESPONSE_TYPE = process.env.QUERY_RESPONSE_TYPE ?? 'graph_and_paths'
const QUERY_HOPS_LIMIT = process.env.QUERY_HOPS_LIMIT !== undefined ? parseInt(process.env.QUERY_HOPS_LIMIT, 10) : 5000000

if (QUERY_DATE_FILTER !== 'path_date_filter' && QUERY_DATE_FILTER !== 'segment_date_filter') {
  console.error(`Invalid QUERY_DATE_FILTER param ${QUERY_DATE_FILTER}. Expected values 'path_date_filter', 'segment_date_filter'.`)
  process.exit(1)
}

if (QUERY_RESPONSE_TYPE !== 'graph_and_paths' && QUERY_RESPONSE_TYPE !== 'graph' && QUERY_RESPONSE_TYPE !== 'count') {
  console.error(`Invalid QUERY_RESPONSE_TYPE param ${QUERY_RESPONSE_TYPE}. Expected values 'graph_and_paths', 'graph', or 'count'.`)
  process.exit(1)
}

const driver = neo4j.driver(`bolt://${HOST}:${PORT}`, undefined, {})

const entities: Array<[string, number]> = fs.readFileSync(`${__dirname}/data.csv`, 'utf-8')
  .split('\n')
  .map((line: string) => {
    const [id, count] = line.split(',')
    return [id, parseInt(count, 10)]
  })
  .filter(([_, count]: [string, number]) => count > MIN_SUPPLY_CHAIN_SIZE)
  // .sort((a: [string], b: [string]) => b[0].localeCompare(a[0]))

let entityIdx = 0
let totalRequestCount = 0
let successCount = 0
let errorCount = 0
let totalEntityCount = 0
const START_TIME = Date.now()
let runCount = 0
let QUERY = QUERY_HOPS_LIMIT ? `USING HOPS LIMIT ${QUERY_HOPS_LIMIT} ` : ''

if (QUERY_DATE_FILTER === 'path_date_filter') {
  QUERY += `
    MATCH (a:company { id: $id })<-[r1:ships_to]-(b)
      WHERE b != a
    WITH a, b, r1,
      r1.hs_code_int AS product,
      r1.shipment_departure AS shipment_departure,
      r1.max_date AS max_date,
      r1.min_date AS min_date
    OPTIONAL MATCH (b)<-[r2:ships_to]-(c)
      WHERE c != b AND c != a
        AND max_date >= r2.min_date AND min_date <= r2.max_date
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN shipment_departure WHERE any(arrival IN r2.shipment_arrival WHERE departure = arrival))` : ''}
        AND (product = r2.hs_code_int OR sayari_c_module.is_product_component(product, r2.hs_code_int))
    WITH a, b, c, r1, r2,
      product,
      r2.hs_code_int AS component,
      r2.shipment_departure AS shipment_departure,
      CASE WHEN max_date < r2.max_date THEN max_date ELSE r2.max_date END AS max_date,
      CASE WHEN min_date > r2.min_date THEN min_date ELSE r2.min_date END AS min_date
    OPTIONAL MATCH (c)<-[r3:ships_to]-(d)
      WHERE d != c AND d != b AND d != a
        AND max_date >= r3.min_date AND min_date <= r3.max_date
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN shipment_departure WHERE any(arrival IN r3.shipment_arrival WHERE departure = arrival))` : ''}
        AND (component = r3.hs_code_int OR sayari_c_module.is_product_component(component, r3.hs_code_int))
    WITH a, b, c, d, collect(r1) AS r1s, collect(r2) AS r2s, r3,
      product,
      r3.hs_code_int AS component,
      r3.shipment_departure AS shipment_departure,
      CASE WHEN max_date < r3.max_date THEN max_date ELSE r3.max_date END AS max_date,
      CASE WHEN min_date > r3.min_date THEN min_date ELSE r3.min_date END AS min_date
    OPTIONAL MATCH (d)<-[r4:ships_to]-(e)
      WHERE e != d AND e != c AND e != b AND e != a
        AND max_date >= r4.min_date AND min_date <= r4.max_date
        ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN shipment_departure WHERE any(arrival IN r4.shipment_arrival WHERE departure = arrival))` : ''}
        AND (component = r4.hs_code_int OR sayari_c_module.is_product_component(component, r4.hs_code_int))
    UNWIND r1s AS r1
    UNWIND r2s AS r2
    WITH
      project([a, b, c, d, e], [r1, r2, r3, r4]) AS graph,
      collect({ product: product, edges: [r1, r2, r3, r4] }) AS paths
  `
} else {
  QUERY += `
    MATCH (a:company { id: $id })<-[r1:ships_to]-(b)
      WHERE b != a 
    WITH a, b, r1.hs_code_int AS product, collect(r1) AS r1s
    OPTIONAL MATCH (b)<-[r2:ships_to]-(c)
      WHERE c != b AND c != a
        AND any(export IN r1s WHERE (
          export.min_date >= r2.min_date AND export.min_date <= r2.max_date
          ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN export.shipment_departure WHERE any(arrival IN r2.shipment_arrival WHERE departure = arrival))` : ''}
          AND (export.hs_code_int = r2.hs_code_int OR sayari_c_module.is_product_component(export.hs_code_int, r2.hs_code_int))
        ))
    WITH a, b, c, product, r1s, collect(r2) AS r2s
    OPTIONAL MATCH (c)<-[r3:ships_to]-(d)
      WHERE d != c AND d != b AND d != a
        AND any(export IN r2s WHERE (
          export.min_date >= r3.min_date AND export.min_date <= r3.max_date
          ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN export.shipment_departure WHERE any(arrival IN r3.shipment_arrival WHERE departure = arrival))` : ''}
          AND (export.hs_code_int = r3.hs_code_int OR sayari_c_module.is_product_component(export.hs_code_int, r3.hs_code_int))
        ))
    WITH a, b, c, d, product, r1s, r2s, collect(r3) AS r3s
    OPTIONAL MATCH (d)<-[r4:ships_to]-(e)
      WHERE e != d AND e != c AND e != b AND e != a
        AND any(export IN r3s WHERE (
          export.min_date >= r4.min_date AND export.min_date <= r4.max_date
          ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL ? `AND any(departure IN export.shipment_departure WHERE any(arrival IN r4.shipment_arrival WHERE departure = arrival))` : ''}
          AND (export.hs_code_int = r4.hs_code_int OR sayari_c_module.is_product_component(export.hs_code_int, r4.hs_code_int))
        ))
    UNWIND r1s AS r1
    UNWIND r2s AS r2
    UNWIND r3s AS r3
    WITH
      project([a, b, c, d, e], [r1, r2, r3, r4]) AS graph,
      collect({ product: product, edges: [r1, r2, r3, r4] }) AS paths
  `
}

if (QUERY_RESPONSE_TYPE === 'graph_and_paths') {
  QUERY += `RETURN 
      extract(edge IN graph.edges | [ID(edge), ID(startNode(edge)), ID(endNode(edge)), edge.hs_code_int, edge.shipment_arrival, edge.shipment_departure, edge.min_date, edge.max_date]) AS edges,
      extract(node IN graph.nodes | [ID(node), node.id, LABELS(node), node.label, node.risk, node.country]) AS nodes,
      extract(path IN paths | { product: path.product, edges: extract(edge IN path.edges | ID(edge)) }) AS paths
  `
} else if (QUERY_RESPONSE_TYPE === 'graph') {
  QUERY += `RETURN 
      extract(edge IN graph.edges | [ID(edge), ID(startNode(edge)), ID(endNode(edge)), edge.hs_code_int, edge.shipment_arrival, edge.shipment_departure, edge.min_date, edge.max_date]) AS edges,
      extract(node IN graph.nodes | [ID(node), node.id, LABELS(node), node.label, node.risk, node.country]) AS nodes
  `
} else {
  QUERY += `RETURN size(graph.nodes) AS count`
}

const queryRunner = async () => {
  while (runCount++ < COUNT) {
    const [id] = entities[entityIdx]
    entityIdx = (entityIdx + 1) % entities.length
    const session = driver.session()
    const t1 = Date.now()

    try {
      let entityCount: number
      let pathCount: number | undefined = undefined
      if (QUERY_RESPONSE_TYPE === 'graph_and_paths' || QUERY_RESPONSE_TYPE === 'graph') {
        const response = await session.run<{ nodes: unknown[], paths: unknown[]}>(QUERY, { id })
        entityCount = response.records[0].get('nodes').length
        if (QUERY_RESPONSE_TYPE === 'graph_and_paths') {
          pathCount = response.records[0].get('paths').length
        }
      } else {
        entityCount = (await session.run<{ count: Integer }>(QUERY, { id })).records[0].get('count').toNumber()
      }

      const totalResponseTime = Date.now() - START_TIME
      totalRequestCount++
      successCount++
      totalEntityCount += entityCount
  
      console.log(
        `Success. Time ${Date.now() - t1}ms. Entity count ${entityCount}.${pathCount !== undefined ? ` Path count ${pathCount}` : ''} ` +
        `(TOTAL: ${totalRequestCount} reqs ${Math.round(totalResponseTime / 1000)} sec) (AVG: latency ${Math.round(totalResponseTime / totalRequestCount)}ms, throughput ${Math.round((successCount / (totalResponseTime / 60000)) * 100) / 100} req/min, entities ${Math.round(totalEntityCount / successCount)}) ` +
        `[Success ${Math.round(successCount/totalRequestCount * 100)}% (${successCount}/${totalRequestCount}) Error ${Math.round(errorCount/totalRequestCount * 100)}% (${errorCount}/${totalRequestCount})] ${id} `
      )
    } catch (err) {
      const totalResponseTime = Date.now() - START_TIME
      totalRequestCount++
      errorCount++
  
      console.error(
        `Error. Time ${Date.now() - t1}ms ` +
        `(TOTAL: ${totalRequestCount} reqs ${Math.round(totalResponseTime / 1000)} sec) (AVG: latency ${Math.round(totalResponseTime / totalRequestCount)}ms, throughput ${Math.round((successCount / (totalResponseTime / 60000)) * 100) / 100} req/min) ` +
        `[Success ${Math.round(successCount/totalRequestCount * 100)}% (${successCount}/${totalRequestCount}) Error ${Math.round(errorCount/totalRequestCount * 100)}% (${errorCount}/${totalRequestCount})] ${id} ` +
        err
      )
    } finally {
      await session.close()
    }
  }
}

console.log(`Querying Memgraph at ${HOST}:${PORT} with concurrency ${CONCURRENCY}, min supply chain size ${MIN_SUPPLY_CHAIN_SIZE}, response type ${QUERY_RESPONSE_TYPE}, hops limit ${QUERY_HOPS_LIMIT ?? '-'}, downstream_departure=upstream_arrival ${QUERY_DOWNSTREAM_DEPARTURE_EQUALS_UPSTREAM_ARRIVAL}`)
console.log(QUERY)

Promise.all(
  Array(CONCURRENCY).fill(null).map(queryRunner)
)
  .then(() => driver.close())
  .catch((err) => console.error(err))
