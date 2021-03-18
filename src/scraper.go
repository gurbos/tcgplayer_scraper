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

	dataSource := GetDataSource()
	dbConn := GetDBConnection(dataSource.DSNString(), logger.Silent)
	Migrate(dataSource.DSNString(), CardImageID{}) // Create temporary database table to hold card image association data
	err := DatabaseConnConfig(dbConn, 10, 10)
	if err != nil {
		log.Fatal(err)
	}

	cmdArgs := os.Args[1:]
	for _, productLineName := range cmdArgs {
		var response *ResponsePayload
		requestInfo := GetRequestPayload(productLineName, "", "", 0)
		for true {
			var tcgpErr *TcgpError
			response, tcgpErr = MakeTcgPlayerRequest(requestInfo.ToJSON(), DefaultTimeout)
			if tcgpErr != nil {
				continue
			}
			break
		}
		tx := WriteProductLineInfo(dbConn, response.Results[0])
		if tx.Error != nil {
			DropTables(dbConn)
			log.Fatal(tx.Error)
		}
		fmt.Println("Product line info written to database.")

		tx = WriteSetInfo(dbConn, response.Results[0].Aggregations)
		if tx.Error != nil {
			DropTables(dbConn)
			log.Fatal(tx.Error)
		}
		fmt.Println("Set info written to database.")

		var wg sync.WaitGroup
		requestChan := make(chan *RequestPayload, numCPUThreads*2) // Buffered channel used to pass RequestPayloads
		{
			cardAttrChan := make(chan []CardAttrs, numCPUThreads*2) // Buffered channel used to pass lists of CardAttrs
			setmap, err := MakeSetMap(dbConn, "yugioh")
			if err != nil {
				DropTables(dbConn)
				log.Fatal(err)
			}

			// Create data request and data write threads
			wg.Add(numCPUThreads)
			for i := 0; i < numCPUThreads; i++ {
				go MakeDataRequest(requestChan, cardAttrChan)
			}
			for i := 0; i < numCPUThreads; i++ {
				go WriteCardInfo(&wg, cardAttrChan, dbConn, setmap)
			}
		}

		setTotal := len(response.Results[0].Aggregations.SetName)
		for i := 0; i < setTotal; i++ {
			requestPayload := GetRequestPayload(
				productLineName,
				response.Results[0].Aggregations.ProductTypeName[0].URLValue,
				response.Results[0].Aggregations.SetName[i].URLValue,
				int(response.Results[0].Aggregations.SetName[i].Count),
			)
			requestChan <- requestPayload
		}
		TerminateCardInfoGoroutines(&wg, requestChan, numCPUThreads) // Send MakeDataRequest goroutines termination value and wait for them to complete

		cardIDChan := make(chan []CardImageID, numCPUThreads*2) // Buffered channel used to pass lists of CardImageID objects
		wg.Add(numCPUThreads)
		for i := 0; i < numCPUThreads; i++ {
			go GetImages(&wg, cardIDChan)
		}

		var count int64 = 0
		var totalCards int64
		var dataSetSize int = 100
		tx = dbConn.Model(CardImageID{}).Count(&totalCards)
		for count < totalCards {
			dataList := make([]CardImageID, 100, 100)
			tx = dbConn.Model(&CardImageID{}).Offset(int(count)).Limit(dataSetSize).Order("new_id ASC").Find(&dataList)
			if tx.Error != nil {
				DropTables(dbConn)
				log.Fatal(tx.Error)
			}
			cardIDChan <- dataList
			count += tx.RowsAffected
			fmt.Println("Images retrieved: ", count)
		}
		TerminateCardImageGoroutines(&wg, cardIDChan, numCPUThreads)
	}
	err = dbConn.Migrator().DropTable(&CardImageID{})
	if err != nil {
		log.Fatal(err)
	}
}
