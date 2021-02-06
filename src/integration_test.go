package main

import (
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm/logger"
)

// TestIntegration :  Integration Test
func TestIntegration(t *testing.T) {

	// Create database tables
	dataSource := GetTestDataSource()
	Migrate(dataSource.DSNString(), &ProductLine{}, &SetInfo{}, &CardInfo{}) // Create database tables

	dbconn := GetDBConnection(dataSource.DSNString(), logger.Silent)
	err := DatabaseConnConfig(dbconn, 10, 10) // Configure the number of open database connections and idle connections

	batchWriteSize := 500 // Size of database write operations

	// Request metadata from the site
	requestInfo := GetRequestPayload("yugioh", "Cards", "duelist-league-promo", 0)
	var responseData *ResponsePayload
	for true {
		responseData, err = MakeTcgPlayerRequest(requestInfo.ToJSON(), DefaultTimeout)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		} else {
			break
		}
	}

	// Write product line data to database
	tx := WriteProductLineInfo(dbconn, responseData.Results[0])
	if tx.Error != nil {
		t.Fatal("WriteProductLine():", tx.Error)
	}

	// Write per product line card sets to database
	tx = WriteSetInfo(dbconn, responseData.Results[0].Aggregations, batchWriteSize)
	if tx.Error != nil {
		t.Fatal("WriteSetInfo():", tx.Error)
	}

	numCPUThread := runtime.NumCPU() * 2
	runtime.GOMAXPROCS(numCPUThread)
	chanBuffSize := 10
	requestChan := make(chan *RequestPayload, chanBuffSize) // Channel for passing data request info
	dataChan := make(chan []CardAttrs, chanBuffSize)        // Channel for passing received data
	var wg sync.WaitGroup
	wg.Add(numCPUThread) // Specify the number of goroutines to wait on

	setMap, err := MakeSetMap(dbconn, "YuGiOh")
	if err != nil {
		log.Fatal("MakeSetMap():", err)
	}

	// Create group of goroutines to request data
	for i := 0; i < numCPUThread; i++ {
		go MakeDataRequest(requestChan, dataChan)
	}

	// Create group of goroutines to write data
	for i := 0; i < numCPUThread; i++ {
		go WriteCardInfo(&wg, dataChan, dbconn, setMap, batchWriteSize)
	}

	// Request card info by card set
	for i := 0; i < 10; i++ {
		requestInfo = GetRequestPayload(
			"yugioh",
			responseData.Results[0].Aggregations.ProductTypeName[0].URLValue,
			responseData.Results[0].Aggregations.SetName[i].URLValue,
			int(responseData.Results[0].Aggregations.SetName[i].Count),
		)
		requestChan <- requestInfo
	}

	// Send go routines termination value
	for i := 0; i < numCPUThread; i++ {
		requestChan <- nil
	}
	wg.Wait() // wait on go routines to complete

}
