package main

import (
	"bufio"
	_ "embed"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

//go:embed data-go-copy.csv
var dataCSV string

var (
	HOST                     = getEnv("HOST", "localhost")
	PORT                     = getEnv("PORT", "7687")
	CONCURRENCY, _           = strconv.Atoi(getEnv("CONCURRENCY", "18"))
	TIMEOUT, _               = strconv.Atoi(getEnv("TIMEOUT", "15"))
	MIN_SUPPLY_CHAIN_SIZE, _ = strconv.Atoi(getEnv("MIN_SUPPLY_CHAIN_SIZE", "5000"))
	entities                 = readCSV()
	entityIdx                = 0
	mutex                    sync.Mutex
)

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func readCSV() [][2]interface{} {
	var entities [][2]interface{}
	scanner := bufio.NewScanner(strings.NewReader(dataCSV))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			continue
		}
		id := parts[0]
		count, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		if count > MIN_SUPPLY_CHAIN_SIZE {
			entities = append(entities, [2]interface{}{id, count})
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %s", err)
	}
	return entities
}

func queryRunner(driver neo4j.Driver, runnerId int) {
	for {
		mutex.Lock()
		entity := entities[entityIdx]
		entityIdx = (entityIdx + 1) % len(entities)
		mutex.Unlock()

		id := entity[0].(string)
		count := entity[1].(int)
		startTime := time.Now()

		session := driver.NewSession(neo4j.SessionConfig{})
		defer session.Close()

		_, err := session.Run(`
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
			RETURN b, c, d, e, collect(r4) AS r4 LIMIT 20000
			QUERY MEMORY LIMIT 5120MB;
		`, map[string]interface{}{"id": id}, neo4j.WithTxTimeout(time.Duration(TIMEOUT)*time.Second))

		if err != nil {
			log.Printf("Query Error. Time %dms. Runner id %d. Entity id %s. Entity supply chain count %d. %s", time.Since(startTime).Milliseconds(), runnerId, id, count, err)
		} else {
			log.Printf("Query Success. Time %dms. Runner id %d. Entity id %s. Entity supply chain count %d.", time.Since(startTime).Milliseconds(), runnerId, id, count)
		}
	}
}

func main() {
	driver, err := neo4j.NewDriver(fmt.Sprintf("bolt://%s:%s", HOST, PORT), neo4j.NoAuth(), func(config *neo4j.Config) {
		config.MaxConnectionLifetime = time.Duration(60) * time.Second
	})
	if err != nil {
		log.Fatalf("Failed to create driver: %s", err)
	}
	defer driver.Close()

	log.Printf("Querying Memgraph at %s:%s with concurrency %d, timeout %dms, min supply chain size %d", HOST, PORT, CONCURRENCY, TIMEOUT, MIN_SUPPLY_CHAIN_SIZE)

	var wg sync.WaitGroup
	for i := 0; i < CONCURRENCY; i++ {
		wg.Add(1)
		go func(runnerId int) {
			defer wg.Done()
			queryRunner(driver, runnerId)
		}(i)
	}
	wg.Wait()
}
