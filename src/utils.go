package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DataSourceName attributes hold informaition about a specific database.
// The information is used to connect to said database.
type DataSourceName struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// DSNString returns a data source identifier string
func (dsn *DataSourceName) DSNString() string {
	format := "%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local"
	return fmt.Sprintf(format, dsn.User, dsn.Password, dsn.Host, dsn.Port, dsn.Database)
}

// Migrate creates the CardInfo database table and returns
func Migrate(dsn string, args ...interface{}) {
	db, err := gorm.Open(
		mysql.Open(dsn),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Error)},
	)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range args {
		err = db.AutoMigrate(m)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// GetDBConnection returns a database connection handle
func GetDBConnection(dsn string, logLevel logger.LogLevel) *gorm.DB {
	db, err := gorm.Open(
		mysql.Open(dsn),
		&gorm.Config{Logger: logger.Default.LogMode(logLevel)},
	)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// DropTables drops all database tables;
func DropTables(db *gorm.DB) {
	gen, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	rows, err := gen.Query(
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=(?)",
		db.Migrator().CurrentDatabase(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var name string
	tx := db.Exec("SET FOREIGN_KEY_CHECKS = 0;") // Disable checks for foreign key relationships
	if tx.Error != nil {
		log.Fatal(tx.Error)
	}
	for rows.Next() {
		rows.Scan(&name)
		tx := db.Exec(fmt.Sprintf("DROP TABLE %s", name))
		if tx.Error != nil {
			log.Fatal(tx.Error)
		}
	}
	_ = db.Exec("SET FOREIGN_KEY_CHECKS = 1;") // Re-enable checks for foreign key relationships
}

func deleteTableRecords(db *gorm.DB) {
	gen, err := db.DB()
	if db.Migrator().HasTable(&CardInfo{}) {
		_, err = gen.Exec("DELETE FROM card_infos;")
		if err != nil {
			log.Println(err)
		}
	}
	if db.Migrator().HasTable(&SetInfo{}) {
		_, err = gen.Exec("DELETE FROM set_infos;")
		if err != nil {
			log.Println(err)
		}
	}
	if db.Migrator().HasTable(&ProductLine{}) {
		_, err = gen.Exec("DELETE FROM product_lines;")
		if err != nil {
			log.Println(err)
		}
	}
}

// GetTestDataSource returns a DataSourceName object that specifies
// the data source used during testing.
func GetTestDataSource() DataSourceName {
	godotenv.Load("/home/gurbos/Projects/golang/scraper/.env")
	dataSource := DataSourceName{
		Host:     os.Getenv("TEST_HOST"),
		Port:     os.Getenv("TEST_PORT"),
		User:     os.Getenv("TEST_USER"),
		Password: os.Getenv("TEST_PASSWD"),
		Database: os.Getenv("TEST_DB"),
	}
	return dataSource
}

// GetDataSource returns a DataSourceName object with that specifies
// the data source used during production.
func GetDataSource() DataSourceName {
	godotenv.Load("/home/gurbos/Projects/golang/tcgplayer_scraper/.env")
	dataSource := DataSourceName{
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWD"),
		Database: os.Getenv("DB_NAME"),
	}
	return dataSource
}

// DatabaseConnConfig Set the max number of open database and idle database connections
func DatabaseConnConfig(db *gorm.DB, numConn int, numIdleConn int) error {
	gen, err := db.DB()
	if err != nil {
		return err
	}
	gen.SetMaxOpenConns(numConn)
	gen.SetMaxIdleConns(numIdleConn)
	return nil
}

func TerminateCardInfoGoroutines(wg *sync.WaitGroup, dataChan chan *RequestPayload, numRoutines int) {
	for i := 0; i < numRoutines; i++ {
		dataChan <- nil
	}
	wg.Wait()
}

func TerminateCardImageGoroutines(wg *sync.WaitGroup, dataChan chan []CardImageID, numRoutines int) {
	for i := 0; i < numRoutines; i++ {
		dataChan <- nil
	}
	wg.Wait()
}

func WriteProductLineInfo(db *gorm.DB, data Result) *gorm.DB {
	var tx *gorm.DB
	for _, elem := range data.Aggregations.ProductLineName {
		if elem.IsActive {
			productLine := ProductLine{
				Title:     elem.Value,
				Name:      elem.URLValue,
				SetCount:  uint(len(data.Aggregations.SetName)),
				CardCount: uint(data.TotalResults),
			}
			tx = db.Create(&productLine)
			break
		}
	}
	return tx
}

func MakeSetMap(dbConn *gorm.DB, productLine string) (map[string]SetInfo, error) {
	var productLineID ProductLineID
	tx := dbConn.Model(ProductLineID{}).Where("name = ?", productLine).Find(&productLineID)
	if tx.Error != nil {
		return nil, tx.Error
	}
	var setCount int64
	tx = dbConn.Model(SetInfo{}).Where("id = ?", productLineID.ID).Count(&setCount)
	if tx.Error != nil {
		return nil, tx.Error
	}
	setInfoList := make([]SetInfo, setCount, setCount)
	setMap := make(map[string]SetInfo)
	for _, elem := range setInfoList {
		setMap[elem.Name] = elem
	}
	return setMap, nil
}

// WriteSetInfo writes the set info data passed into the data parameter and inserts
// it into the set info database table.
func WriteSetInfo(db *gorm.DB, data aggregation, batchWriteSize int) (tx *gorm.DB) {
	var productLineID ProductLineID
	var productLineName string

	// Get ProductLineID from active ProuctLineName
	for _, val := range data.ProductLineName {
		if val.IsActive {
			productLineName = val.Value
			tx = db.Model(&ProductLine{}).Where("Name = (?)", productLineName).First(&productLineID)
			if tx.Error != nil {
				return
			}
			break
		}
	}

	setInfoList := makeSetInfoList(productLineID.ID, data.SetName)
	tx = db.CreateInBatches(setInfoList, batchWriteSize)
	return
}

// MakeDataRequest is meant to be executed as a goroutine. It receives RequestPayload objects through the
// the channel passed in the requestChan parameter, makes the request and passes the response to another
// goroutine, through the channel passed in the dataChan parameter to, which processes the data.
func MakeDataRequest(requestChan chan *RequestPayload, dataChan chan []CardAttrs) {
	var ri *RequestPayload
	var rd *ResponsePayload
	var err error
	for true {
		ri = <-requestChan
		if ri != nil {
			for true {
				rd, err = MakeTcgPlayerRequest(ri.ToJSON(), 50)
				if err != nil {
					continue
				}
				break
			}
			if rd.Results[0].TotalResults != 0 {
				dataChan <- rd.Results[0].Results
				fmt.Printf("%-60s  %d\n", ri.Filters.Term.SetName[0], ri.Size)
			}
			continue
		}
		dataChan <- nil
		break
	}
}

// WriteCardInfo reads card info data from a channel and inserts it into the card info database table.
func WriteCardInfo(wg *sync.WaitGroup, dataChan chan []CardAttrs, db *gorm.DB, setMap map[string]SetInfo, batchWriteSize int) {
	defer wg.Done()

	for true {
		data := <-dataChan
		if data != nil {
			cardInfoList, err := makeCardInfoList(data, setMap)
			if err != nil {
				log.Fatal(err)
			}
			for true {
				tx := db.Create(cardInfoList) // db.CreateInBatches(cardInfoList, batchWriteSize)
				if tx.Error != nil {
					fmt.Println(tx.Error)
					if strings.Index(tx.Error.Error(), "Duplicate entry") != -1 {
						keys := parseDuplicateValue(tx.Error.Error())
						cardInfoList = removeEntry(cardInfoList, keys[0], keys[1], keys[2], keys[3])
						continue
					}
				}
				break // Break if no database error
			}
			continue // Retry database write after error handling
		}
		break
	}
}

// makeSetInfoList returns a list of SetInfo structures. The db parameter is a
// open database conntection handle used to get foreign key information from corresponding
// tables and, together with the data passed in the attr parameter, is used to Write the
// fields of the SetInfo structures in the returned list.
func makeSetInfoList(productLineID uint, data []itemInfo) []SetInfo {
	length := len(data)
	list := make([]SetInfo, length, length)
	for i := 0; i < length; i++ {
		list[i].Title = data[i].Value
		list[i].Name = data[i].URLValue
		list[i].Count = uint16(data[i].Count)
		list[i].ProductLineID = productLineID
	}
	return list
}

// makeCardInfoList returns a list of CardInfo structures. The db parameter is a
// open database connection handle used to get foreign ey information from corresponding
// tables and, together with the data passed in the attr parameter, is used to Write the
// fields of the CardInfo structures in the returned list.
func makeCardInfoList(attr []CardAttrs, setMap map[string]SetInfo) ([]CardInfo, error) {
	cardInfo := make([]CardInfo, len(attr))
	for i := 0; i < len(attr); i++ {
		cardInfo[i].Attack = attr[i].CustomAttributes.Attack

		// attr[i].CustomAttributes.Attribute is a variable length list of strings
		if len(attr[i].CustomAttributes.Attribute) == 0 {
			cardInfo[i].Attribute = ""
		} else {
			cardInfo[i].Attribute = strings.Join(attr[i].CustomAttributes.Attribute, ",")
		}

		// attr[i].CustomAttributes.CardType is a variable length list of strings
		cardInfo[i].CardType = strings.Join(attr[i].CustomAttributes.MonsterType, ",")

		cardInfo[i].CardTypeB = attr[i].CustomAttributes.CardTypeB
		cardInfo[i].Defense = attr[i].CustomAttributes.Defense
		cardInfo[i].Description = strings.TrimSpace(attr[i].CustomAttributes.Description)
		cardInfo[i].LinkArrows = strings.Join(attr[i].CustomAttributes.LinkArrows, ",")

		// cardInfo[i].ID = uint(attr[i].ProductID)
		cardInfo[i].Level = attr[i].CustomAttributes.Level
		cardInfo[i].MarketPrice = attr[i].MarketPrice

		temp := strings.Join(attr[i].CustomAttributes.MonsterType, ",")
		cardInfo[i].MonsterType = strings.TrimSpace(temp)

		cardInfo[i].Name = attr[i].ProductName
		cardInfo[i].Number = attr[i].CustomAttributes.Number
		cardInfo[i].Rarity = attr[i].RarityName
		cardInfo[i].SetID = setMap[attr[i].SetName].ID                    // Set set infoforeign key
		cardInfo[i].ProductLineID = setMap[attr[i].SetName].ProductLineID // Set product line foreign key
		cardInfo[i].ProductID = uint(attr[i].ProductID)                   // Used to identify associated card image
	}
	return cardInfo, nil
}

func parseDuplicateValue(errstr string) []string {
	s := strings.Index(errstr, "'") + 1
	e := strings.Index(errstr[s:len(errstr)-1], "'")
	e += s
	errstr = errstr[s:e]
	keys := strings.Split(errstr, "-")
	return []string{keys[0] + "-" + keys[1], keys[2], keys[3], keys[4]}
}

func removeEntry(list []CardInfo, number string, name string, rarity string, setID string) []CardInfo {
	length := len(list)
	temp, _ := strconv.Atoi(setID)
	sID := uint(temp)
	for i := 0; i < length; i++ {
		if list[i].Number == number && list[i].Name == name && list[i].Rarity == rarity && list[i].SetID == sID {
			list[i] = list[length-1]
			return list[0 : length-1]
		}
	}
	return list
}

func GetImages(wg *sync.WaitGroup, dataChan chan []CardImageID) {
	defer wg.Done()

	var response *http.Response
	godotenv.Load("/home/gurbos/Projects/golang/tcgplayer_scraper/.env")
	imgDir := os.Getenv("IMG_DIR")
	var remoteFileName string
	var url string
	client := http.Client{}
	for true {
		data := <-dataChan
		if data != nil {
			for i := 0; i < len(data); i++ {
				remoteFileName = strconv.Itoa(int(data[i].ProductID)) + "_200w.jpg" // Create remote filename from cardInfo.ProductID
				url = TcgPlayerImageURL + "/" + remoteFileName                      // Build filename url from resource domain and remote filename
				request, err := http.NewRequest(http.MethodGet, url, nil)           // Create http request object
				if err != nil {
					log.Fatal(err)
				}

				for true {
					response, err = client.Do(request) // Make request and receive rescponse
					if err != nil {
						continue
					}
					break
				}

				buff, err := ioutil.ReadAll(response.Body) // Read image file from http response body
				if err != nil {
					fmt.Println(err)
				}
				response.Body.Close()

				path := path.Join(imgDir, strconv.Itoa(int(data[i].ID))+"_200w.jpg") // Card image file is named using the corresponding card id number
				file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0755)        // Create image file
				if err != nil {
					log.Fatal(err)
				}

				_, err = file.Write(buff) // Write image contents to file
				if err != nil {
					fmt.Println(err)
				}
				file.Close()
			}
		} else {
			break
		}
	}
}

// ProductLineID used when only requesting ProductLine.ID field from corresponding database table.
type ProductLineID struct {
	ID uint
}

// SetInfoID used when only requesting SetInfo.ID field from corresponding database table.
type SetInfoID struct {
	ID uint
}

// CardImageID is used when only the ID and ProductID fields need to be retrieved
// from the CardInfo database table. Used in the GetImages goroutine.
type CardImageID struct {
	ID        uint // Local image ID
	ProductID uint // Remote image ID
}
