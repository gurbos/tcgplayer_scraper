package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"

	tcm "github.com/gurbos/tcmodels"
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
	format := "%s:%s@tcp(%s:%s)/%s" //?charset=utf8mb4&parseTime=True&loc=Local"
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
	if db.Migrator().HasTable(&tcm.YuGiOhCardInfo{}) {
		_, err = gen.Exec("DELETE FROM card_infos;")
		if err != nil {
			log.Println(err)
		}
	}
	if db.Migrator().HasTable(&tcm.SetInfo{}) {
		_, err = gen.Exec("DELETE FROM set_infos;")
		if err != nil {
			log.Println(err)
		}
	}
	if db.Migrator().HasTable(&tcm.ProductLine{}) {
		_, err = gen.Exec("DELETE FROM product_lines;")
		if err != nil {
			log.Println(err)
		}
	}
}

// GetDataSource returns a DataSourceName object with that specifies
// the data source used during production.
func GetDataSource() DataSourceName {
	godotenv.Load("/home/gurbos/Projects/golang/scraper/.env")
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
			productLine := tcm.ProductLine{
				Name:      elem.Value,
				URLName:   elem.URLValue,
				SetCount:  uint(len(data.Aggregations.SetName)),
				CardCount: uint(data.TotalResults),
			}
			tx = db.Create(&productLine)
			break
		}
	}
	return tx
}

func MakeSetMap(dbConn *gorm.DB, productLine string) (map[string]tcm.SetInfo, error) {
	var productLineID ProductLineID
	tx := dbConn.Model(tcm.ProductLine{}).Where("name = ?", productLine).Find(&productLineID)
	if tx.Error != nil {
		return nil, tx.Error
	}
	var setCount int64
	tx = dbConn.Model(tcm.SetInfo{}).Where("product_line_id = ?", productLineID.ID).Count(&setCount)
	if tx.Error != nil {
		return nil, tx.Error
	}
	setInfoList := make([]tcm.SetInfo, setCount, setCount)
	tx = dbConn.Model(tcm.SetInfo{}).Where("product_line_id = ?", productLineID.ID).Find(&setInfoList)
	setMap := make(map[string]tcm.SetInfo)
	for _, elem := range setInfoList {
		setMap[elem.Name] = elem
	}
	return setMap, nil
}

// WriteSetInfo writes the set info data passed into the data parameter and inserts
// it into the set info database table.
func WriteSetInfo(db *gorm.DB, data aggregation) (tx *gorm.DB) {
	var productLineID ProductLineID
	var productLineName string

	// Get ProductLineID from active ProuctLineName
	for _, val := range data.ProductLineName {
		if val.IsActive {
			productLineName = val.Value
			tx = db.Model(&tcm.ProductLine{}).Where("Name = (?)", productLineName).First(&productLineID)
			if tx.Error != nil {
				return
			}
			break
		}
	}

	setInfoList := makeSetInfoList(productLineID.ID, data.SetName)
	tx = db.Create(setInfoList)
	return
}

// MakeDataRequest is meant to be executed as a goroutine. It receives RequestPayload objects through the
// the channel passed in the requestChan parameter, makes the request and passes the response to another
// goroutine, through the channel passed in the dataChan parameter to, which processes the data.
func MakeDataRequest(requestChan chan *RequestPayload, dataChan chan []CardAttrs) {
	var ri *RequestPayload
	var rd *ResponsePayload
	var tcgpErr *TcgpError
	for true {
		ri = <-requestChan
		if ri != nil {
			for true {
				rd, tcgpErr = MakeTcgPlayerRequest(ri.ToJSON(), 50)
				if tcgpErr != nil {
					continue
				}
				break
			}
			if rd.Results[0].TotalResults != 0 {
				dataChan <- rd.Results[0].Results
			}
			continue
		}
		dataChan <- nil
		break
	}
}

// WriteCardInfo reads card info data from a channel and writes it to the corresponding database table.
func WriteCardInfo(wg *sync.WaitGroup, dataChan chan []CardAttrs, db *gorm.DB, setMap map[string]tcm.SetInfo) {
	defer wg.Done()

	for true {
		data := <-dataChan
		if data != nil {
			cardInfoList, err := makeCardInfoList(data, setMap)
			if err != nil {
				log.Fatal(err)
			}
			for true {
				tx := writeCardInfo(db, cardInfoList)
				if tx.Error != nil {
					fmt.Println(tx.Error)
					if strings.Index(tx.Error.Error(), "Duplicate entry") != -1 {
						// keys := parseDuplicateValue(tx.Error.Error())
						// cardInfoList = removeEntry(cardInfoList, keys[0], keys[1], keys[2], keys[3])
						continue
					}
				}
				productIDList, _ := makeCardImageIDList(data, cardInfoList)
				tx = db.Create(productIDList)
				if tx.Error != nil {
					log.Fatal(tx.Error)
				}
				break // Break if no database write error
			}
			fmt.Printf("%-60s  %d\n", data[0].SetName, reflect.ValueOf(cardInfoList).Len())
			continue // Retry database write after error handling
		}
		break
	}
}

func writeCardInfo(dbconn *gorm.DB, cards interface{}) (tx *gorm.DB) {
	switch cards.(type) {
	case []tcm.YuGiOhCardInfo:
		list := reflect.ValueOf(cards).Interface().([]tcm.YuGiOhCardInfo)
		tx = dbconn.Create(&list)
	}
	return tx
}

// makeSetInfoList returns a list of SetInfo structures. The db parameter is a
// open database conntection handle used to get foreign key information from corresponding
// tables and, together with the data passed in the attr parameter, is used to Write the
// fields of the SetInfo structures in the returned list.
func makeSetInfoList(productLineID uint, data []itemInfo) []tcm.SetInfo {
	length := len(data)
	list := make([]tcm.SetInfo, length, length)
	for i := 0; i < length; i++ {
		list[i].Name = data[i].Value
		list[i].URLName = data[i].URLValue
		list[i].CardCount = uint(data[i].Count)
		list[i].ProductLineID = productLineID
	}
	return list
}

// makeCardInfoList returns a list of CardInfo structures. The db parameter is a
// open database connection handle used to get foreign ey information from corresponding
// tables and, together with the data passed in the attr parameter, is used to Write the
// fields of the CardInfo structures in the returned list.
func makeCardInfoList(attr []CardAttrs, setMap map[string]tcm.SetInfo) (interface{}, error) {
	var cardInfoList interface{}
	switch attr[0].ProductLineURLName {
	case "YuGiOh":
		cardInfos := make([]tcm.YuGiOhCardInfo, len(attr))
		for i := 0; i < len(attr); i++ {
			cardInfos[i].Attack = attr[i].CustomAttributes.Attack

			// attr[i].CustomAttributes.Attribute is a variable length list of strings
			if len(attr[i].CustomAttributes.Attribute) == 0 {
				cardInfos[i].Attribute = ""
			} else {
				cardInfos[i].Attribute = strings.Join(attr[i].CustomAttributes.Attribute, ",")
			}

			// attr[i].CustomAttributes.CardType is a variable length list of strings
			cardInfos[i].CardType = strings.Join(attr[i].CustomAttributes.MonsterType, ",")

			cardInfos[i].CardTypeB = attr[i].CustomAttributes.CardTypeB
			cardInfos[i].Defense = attr[i].CustomAttributes.Defense
			cardInfos[i].Description = strings.TrimSpace(attr[i].CustomAttributes.Description)
			cardInfos[i].LinkArrows = strings.Join(attr[i].CustomAttributes.LinkArrows, ",")

			// cardInfos[i].ID = uint(attr[i].ProductID)
			cardInfos[i].Level = attr[i].CustomAttributes.Level

			temp := strings.Join(attr[i].CustomAttributes.MonsterType, ",")
			cardInfos[i].MonsterType = strings.TrimSpace(temp)

			cardInfos[i].Name = attr[i].ProductName
			cardInfos[i].URLName = attr[i].ProductURLName
			cardInfos[i].Number = attr[i].CustomAttributes.Number
			cardInfos[i].Rarity = attr[i].RarityName
			cardInfos[i].SetID = setMap[attr[i].SetName].ID                    // Set set infoforeign key
			cardInfos[i].ProductLineID = setMap[attr[i].SetName].ProductLineID // Set product line foreign key
		}
		cardInfoList = cardInfos
	}

	return cardInfoList, nil
}

func makeCardImageIDList(attrList []CardAttrs, cardInfoList interface{}) ([]CardImageID, error) {
	listVal := reflect.ValueOf(cardInfoList)
	idList := make([]CardImageID, listVal.Len(), listVal.Len())
	switch cardInfoList.(type) {
	case []tcm.YuGiOhCardInfo:
		vals := reflect.ValueOf(cardInfoList).Interface().([]tcm.YuGiOhCardInfo)
		for i := 0; i < listVal.Len(); i++ {
			idList[i].OldID = uint(attrList[i].ProductID)
			idList[i].NewID = vals[i].ID
			idList[i].ProductLineID = vals[i].ProductLineID
		}
	}
	return idList, nil
}

func parseDuplicateValue(errstr string) []string {
	s := strings.Index(errstr, "'") + 1
	e := strings.Index(errstr[s:len(errstr)-1], "'")
	e += s
	errstr = errstr[s:e]
	keys := strings.Split(errstr, "-")
	return []string{keys[0] + "-" + keys[1], keys[2], keys[3], keys[4]}
}

func removeEntry(list []tcm.YuGiOhCardInfo, number string, name string, rarity string, setID string) []tcm.YuGiOhCardInfo {
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
	godotenv.Load()
	imgDir := os.Getenv("IMG_DIR")
	var remoteFileName string
	var url string
	client := http.Client{}
	for true {
		data := <-dataChan
		if data != nil {
			for i := 0; i < len(data); i++ {
				remoteFileName = strconv.Itoa(int(data[i].OldID)) + "_200w.jpg" // Create remote filename from cardInfo.ProductID
				url = TcgpImageURL + "/" + remoteFileName                       // Build filename url from resource domain and remote filename
				request, err := http.NewRequest(http.MethodGet, url, nil)       // Create http request object
				if err != nil {
					log.Fatal(err)
				}

				for true {
					response, err = client.Do(request) // Make request and receive response
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

				path := path.Join(imgDir, strconv.Itoa(int(data[i].NewID))+"_200w.jpg") // Card image file is named using the corresponding card id number
				file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0755)           // Create image file
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
