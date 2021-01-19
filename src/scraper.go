package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"

	"gorm.io/gorm/logger"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpuprofile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	// Configure parallelism
	numCPUThreads := runtime.NumCPU() * 2 // Get number of logical processors
	runtime.GOMAXPROCS(numCPUThreads)     // Set max number of logical processors that can execute in parallel

	dataSource := GetTestDataSource()
	dbConn := GetDBConnection(dataSource.DSNString(), logger.Silent)
	Migrate(dataSource.DSNString(), &ProductLine{}, &SetInfo{}, &CardInfo{})
	err := DatabaseConnConfig(dbConn, 10, 10)
	if err != nil {
		log.Fatal(err)
	}

	batchSize := 500 // Database batch write size

	// Request product line and card set info from site
	var response *ResponsePayload
	requestInfo := GetRequestPayload("yugioh", "", "", 0)
	for true {
		response, err = MakeTcgPlayerRequest(requestInfo.ToJSON(), DefaultTimeout)
		if err != nil {
			continue
		}
		break
	}

	tx := WriteProductLine(dbConn, response.Results[0].Aggregations)
	if tx.Error != nil {
		DropTables(dbConn)
		log.Fatal(tx.Error)
	}

	tx = WriteSetInfo(dbConn, response.Results[0].Aggregations, batchSize)
	if tx.Error != nil {
		DropTables(dbConn)
		log.Fatal(tx.Error)
	}

	var wg sync.WaitGroup
	requestChan := make(chan *RequestPayload, numCPUThreads*2) // Buffered channel used to pass RequestPayloads
	cardAttrChan := make(chan []CardAttrs, numCPUThreads*2)    // Buffered channel used to pass lists of CardAttrs
	cardIDChan := make(chan []CardImageID, numCPUThreads*2)    // Buffered channel used to pass lists of CardImagesID objects

	setmap, err := MakeSetMap(dbConn, "yugioh")
	if err != nil {
		DropTables(dbConn)
		log.Fatal(err)
	}

	wg.Add(numCPUThreads)
	for i := 0; i < numCPUThreads; i++ {
		go MakeDataRequest(requestChan, cardAttrChan)
	}
	for i := 0; i < numCPUThreads; i++ {
		go WriteCardInfo(&wg, cardAttrChan, dbConn, setmap, batchSize)
	}

	setTotal := len(response.Results[0].Aggregations.SetName)
	for i := 0; i < setTotal; i++ {
		requestPayload := GetRequestPayload(
			"yugioh",
			response.Results[0].Aggregations.ProductTypeName[0].URLValue,
			response.Results[0].Aggregations.SetName[i].URLValue,
			int(response.Results[0].Aggregations.SetName[i].Count),
		)
		requestChan <- requestPayload
	}
	TerminateCardInfoGoroutines(&wg, requestChan, numCPUThreads) // Send MakeDataRequest goroutines termination value and wait for them to complete

	wg.Add(numCPUThreads)
	for i := 0; i < numCPUThreads; i++ {
		go GetImages(&wg, cardIDChan)
	}

	var count int64 = 0
	var cardCount int64
	tx = dbConn.Model(CardInfo{}).Count(&cardCount)
	for count < cardCount {
		dataList := make([]CardImageID, 100, 100)
		tx = dbConn.Model(CardInfo{}).Where("ID > ?", count).Limit(100).Find(&dataList)
		cardIDChan <- dataList
		count += tx.RowsAffected
		fmt.Println("Images retrieved: ", count)
	}
	TerminateCardImageGoroutines(&wg, cardIDChan, numCPUThreads)
}
