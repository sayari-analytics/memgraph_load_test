import * as fs from 'fs'
import neo4j from 'neo4j-driver'

const HOST = process.env.HOST ?? 'localhost'
const PORT = process.env.PORT ?? '7687'
const CONCURRENCY = process.env.CONCURRENCY !== undefined ? parseInt(process.env.CONCURRENCY, 10) : 18
const TIMEOUT = process.env.TIMEOUT !== undefined ? parseInt(process.env.TIMEOUT) : 15000
const MIN_SUPPLY_CHAIN_SIZE = process.env.MIN_SUPPLY_CHAIN_SIZE !== undefined ? parseInt(process.env.MIN_SUPPLY_CHAIN_SIZE, 10) : 2000

const driver = neo4j.driver(
  `bolt://${HOST}:${PORT}`,
  undefined, 
  { maxConnectionLifetime: 60_000, connectionLivenessCheckTimeout: 15_000 }
)

const entities: Array<[string, number]> = fs.readFileSync(`${__dirname}/data.csv`, 'utf-8')
  .split('\n')
  .map((line: string) => {
    const [id, count] = line.split(',')
    return [id, parseInt(count, 10)]
  })
  .filter(([_, count]: [string, number]) => count > MIN_SUPPLY_CHAIN_SIZE)
  .sort((a: [string], b: [string]) => b[0].localeCompare(a[0]))
let entityIdx = 0
let totalResponseTime = 0
let totalRequestCount = 0
let successCount = 0
let errorCount = 0

const queryRunner = async (runnerId: number) => {
  while (true) {
    const [id, count] = entities[entityIdx]
    entityIdx = (entityIdx + 1) % entities.length
    const time = Date.now()
    const session = driver.session()

    try {
      const result = await session.run(`
        MATCH (a:company { id: $id })<-[r1:ships_to]-(b)
        WHERE b != a 
        WITH a, b, collect(r1) AS r1
          OPTIONAL MATCH (b)<-[r2:ships_to]-(c)
          WHERE c != b AND c != a AND any(export IN r1
          WHERE (
          export.min_date >= r2.min_date AND export.min_date <= r2.max_date AND
          product_map.is_component(export.hs_code, r2.hs_code)
          ))
        WITH a, b, c, r1, collect(r2) AS r2
          OPTIONAL MATCH (c)<-[r3:ships_to]-(d)
          WHERE d != c AND d != b AND d != a AND any(export IN r2
          WHERE (
          export.min_date >= r3.min_date AND export.min_date <= r3.max_date AND
          product_map.is_component(export.hs_code, r3.hs_code)
          ))
        WITH a, b, c, d, r1, r2, collect(r3) AS r3
          OPTIONAL MATCH (d)<-[r4:ships_to]-(e)
          WHERE e != d AND e != c AND e != b AND e != a AND any(export IN r3
          WHERE (
          export.min_date >= r4.min_date AND export.min_date <= r4.max_date AND
          product_map.is_component(export.hs_code, r4.hs_code)
          ))
        WITH collect(distinct b) AS b, collect(distinct c) AS c, collect(distinct d) AS d, collect(distinct e) AS e, r1, r2, r3, collect(r4) AS r4 LIMIT 20000
        RETURN
          collect(extract(edge in r1 | [ID(startNode(edge)), ID(endNode(edge)), edge.hs_code_int, edge.arrival_date])) AS r1,
          collect(extract(node in b | [ID(node), node.id, node.label, node.risk])) AS b,
          collect(extract(edge in r2 | [ID(startNode(edge)), ID(endNode(edge)), edge.hs_code_int, edge.arrival_date])) AS r2,
          collect(extract(node in c | [ID(node), node.id, node.label, node.risk])) AS c,
          collect(extract(edge in r3 | [ID(startNode(edge)), ID(endNode(edge)), edge.hs_code_int, edge.arrival_date])) AS r3,
          collect(extract(node in d | [ID(node), node.id, node.label, node.risk])) AS d,
          collect(extract(edge in r4 | [ID(startNode(edge)), ID(endNode(edge)), edge.hs_code_int, edge.arrival_date])) AS r4,
          collect(extract(node in e | [ID(node), node.id, node.label, node.risk])) AS e
        QUERY MEMORY LIMIT 5120MB;
      `, { id }, { timeout: TIMEOUT })
      const dt = Date.now() - time
      totalResponseTime += dt
      totalRequestCount++
      successCount++
      console.log(
        `Query Success. Time ${dt}. Runner ${runnerId}/${CONCURRENCY - 1}. Entity id ${id}. Entity supply chain count ${count}. Result count ${result.records[0].get('e').length}. ` +
        `(Avg Response Time ${Math.round(totalResponseTime / totalRequestCount)}ms) ` +
        `[Success ${Math.round(successCount/totalRequestCount * 100)}% (${successCount}/${totalRequestCount}) Error ${Math.round(errorCount/totalRequestCount * 100)}% (${errorCount}/${totalRequestCount})]`
      )
    } catch (err) {
      const dt = Date.now() - time
      totalResponseTime += dt
      totalRequestCount++
      errorCount++
      console.error(
        `Query Error. Time ${dt}. Runner ${runnerId}/${CONCURRENCY - 1}. Entity id ${id}. Entity supply chain count ${count}. ` +
        `(Avg Response Time ${Math.round(totalResponseTime / totalRequestCount)}ms) ` +
        `[Success ${Math.round(successCount/totalRequestCount * 100)}% (${successCount}/${totalRequestCount}) Error ${Math.round(errorCount/totalRequestCount * 100)}% (${errorCount}/${totalRequestCount})]` +
        `${err}`
      )
    } finally {
      await session.close()
    }
  }
}

console.log(`Querying Memgraph at ${HOST}:${PORT} with concurrency ${CONCURRENCY}, timeout ${TIMEOUT}ms, min supply chain size ${MIN_SUPPLY_CHAIN_SIZE}`)

Promise.all(
  Array(CONCURRENCY).fill(null).map((_, idx) => {
    return queryRunner(idx)
  })
)
  .catch((err) => console.error(err))
