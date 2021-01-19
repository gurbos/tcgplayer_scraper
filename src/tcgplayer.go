package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/gorm"
)

// TcgPlayerDataURL is the base URL where card info can be located
var TcgPlayerDataURL string = "https://mpapi.tcgplayer.com/v2/search/request?q=&isList=false"

// TcgPlayerImageURL is the base URL where card images can be located
var TcgPlayerImageURL string = "https://tcgplayer-cdn.tcgplayer.com/product"

// MaxResultSetSize represends the maximum number of cards per result set
var MaxResultSetSize int = 800

// DefaultTimeout is initial HTTP request timeout in seconds
var DefaultTimeout int = 40

/*****************************************************************************************/

// RequestPayload attributes identify recources to be request.
// They are encoded into a json string and used as the body of an http.Request.
type RequestPayload struct {
	Algorithm     string        `json:"algorithm"`
	From          int           `json:"from"`
	Size          int           `json:"size"`
	Filters       filter        `json:"filters"`
	ListingSearch listingSearch `json:"listingSearch"`
	Context       context       `json:"context"`
	Sort          sort          `json:"sort"`
}

type filter struct {
	Term  term   `json:"term"`
	Range _range `json:"range"`
}

type listingSearch struct {
	Filters _filter `json:"filters"`
}

type context struct {
	Cart            cart   `json:"cart"`
	ShippingCountry string `json:"shippingCountry"`
}

type cart struct{}

type term struct {
	ProductLineName []string `json:"productLineName"`
	ProductTypeName []string `json:"productTypeName"`
	SetName         []string `json:"setName"`
}

type exclude struct {
	ChannelExclusion int `json:"channelExclusion"`
}

type _filter struct {
	Term    _term   `json:"term"`
	Range   _range  `json:"range"`
	Exclude exclude `json:"exclude"`
}

type _term struct{}
type _range struct{}
type sort struct{}

// ToJSON returns a json encoded string from the attributes
// of the associated RequestData structure. An empty string is returned
// upon error.
func (rd *RequestPayload) ToJSON() string {
	buffer, err := json.Marshal(rd)
	if err != nil {
		return ""
	}
	return string(buffer)
}

// DecodeJSON populates the fields of the RequestInfo data structure with the decoded values of the JSON string
func (rd *RequestPayload) DecodeJSON(js string) error {
	err := json.Unmarshal([]byte(js), rd)
	return err
}

/*****************************************************************************************/

// ResponsePayload a structure used to hold values of a the decoded
// json string returned by a request to TcgPlayerDataURL.
type ResponsePayload struct {
	Errors  []string
	Results []result
}

type result struct {
	Aggregations aggregation
	Algorithm    string
	ResultID     string
	Results      []CardAttrs
	TotalResults float32
}

type aggregation struct {
	CardType        []itemInfo
	ProductLineName []itemInfo
	ProductTypeName []itemInfo
	Rarityname      []itemInfo
	SetName         []itemInfo
}

// CardAttrs fields represent all the attributes of a single card.
type CardAttrs struct {
	CustomAttributes        customAttr
	FoilOnly                bool
	Listings                []interface{}
	LowestPrice             float32
	LowestPriceWithShipping float32
	MaxFulfillableQuantity  float32
	MarketPrice             float32
	ProductID               float32
	ProductLineID           float32
	ProductLineName         string
	ProductLineURLName      string
	ProductName             string
	ProductURLName          string
	RarityName              string
	Score                   float32
	SetID                   float32
	SetName                 string
	SetURLName              string
	TotaListings            int
}

type itemInfo struct {
	Count    float32
	IsActive bool
	URLValue string
	Value    string
}

type customAttr struct {
	Attack       string
	Attribute    []string
	CardType     []string
	CardTypeB    string
	Defense      string
	Description  string
	LinkArrows   []string
	LinkRating   string
	Level        string
	MonsterType  []string
	Number       string
	RarityDbName string
}

/*****************************************************************************************/

// GetRequestPayload returns a RequestData struture which specifies data being requested by an http request.
// The data is used as the body of an http.Request structure.
func GetRequestPayload(productLine string, productType string, setName string, resultSize int) *RequestPayload {
	var requestData RequestPayload

	// Create slices for array fields
	requestData.Filters.Term.ProductLineName = make([]string, 0, 0)
	requestData.Filters.Term.ProductTypeName = make([]string, 0, 0)
	requestData.Filters.Term.SetName = make([]string, 0, 0)
	requestData.From = 0

	if productLine != "" {
		requestData.Filters.Term.ProductLineName = append(requestData.Filters.Term.ProductLineName, strings.ToLower(productLine))
	}
	if productType != "" {
		requestData.Filters.Term.ProductTypeName = append(requestData.Filters.Term.ProductTypeName, strings.Title(productType))
	}
	if setName != "" {
		requestData.Filters.Term.SetName = append(requestData.Filters.Term.SetName, setName)
	}

	requestData.Algorithm = ""
	requestData.Size = resultSize
	requestData.ListingSearch.Filters.Exclude.ChannelExclusion = 0
	requestData.Context.ShippingCountry = "US"
	return &requestData
}

// MakeDataRequest
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

// tcgPlayerHTTPRequest creates a new http.Request object and initializes its fields
// with information specifying the data being requested.
func tcgPlayerHTTPRequest(method string, url string, body string) *http.Request {
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		log.Fatal("initializedHTTPRequest() --> http.NewRequest(): " + err.Error())
	}
	request.Header.Set("request-url", "https://mpapi.tcgplayer.com/v2/search/request?q=&isList=false")
	request.Header.Set("authority", "mpapi.tcgplayer.com")
	request.Header.Set("path", "/v2/search/request?q=&isList=false")
	request.Header.Set("scheme", "https")
	request.Header.Set("accept", "application/json, text/plain, */*")
	request.Header.Set("accept-language", "en-US,en;q=0.9")
	request.Header.Set("content-type", "application/json;charset=UTF-8")
	request.Header.Set("origin", "https://www.tcgplayer.com")
	request.Header.Set("referer", "https://www.tcgplayer.com/")
	request.Header.Set(
		"user-agent",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "+
			"(KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
	)
	request.Header.Set("content-length", strconv.Itoa(len(body)))
	return request
}

func MakeTcgPlayerRequest(requestBody string, timeout int) (*ResponsePayload, error) {
	request := tcgPlayerHTTPRequest(http.MethodPost, TcgPlayerDataURL, requestBody)
	client := http.Client{Timeout: time.Duration(timeout) * time.Second}

	// Make http request and receive rescponse
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	// Check http status codes for errors
	if response.StatusCode >= 300 {
		return nil, errors.New("HTTP Status: " + response.Status)
	}

	// Read json response body into buffer
	buff, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var payload ResponsePayload
	err = json.Unmarshal(buff, &payload) // Decode json buffer into ResponsePayload structure
	if err != nil {
		return nil, err
	}
	return &payload, nil
}

func WriteProductLine(db *gorm.DB, data aggregation) *gorm.DB {
	var tx *gorm.DB
	for _, elem := range data.ProductLineName {
		if elem.IsActive {
			productLine := ProductLine{Title: elem.Value, Name: elem.URLValue}
			tx = db.Create(&productLine)
			break
		}
	}
	return tx
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
		list[i].Name = data[i].Value
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

func MakeSetMap(db *gorm.DB, productLineName string) (map[string]SetInfo, error) {
	var productLineID ProductLineID
	var setCount int64

	// Query database for product line id
	tx := db.Model(&ProductLine{}).Where("Name = (?)", productLineName).First(&productLineID)
	if tx.Error != nil {
		return nil, tx.Error
	}

	// Query database product line set count
	tx = db.Model(&SetInfo{}).Where("product_line_id = (?)", productLineID.ID).Count(&setCount)
	if tx.Error != nil {
		return nil, tx.Error
	}

	// Query database for set info
	setList := make([]SetInfo, setCount, setCount)
	tx = db.Model(&SetInfo{}).Where("product_line_id = (?)", productLineID.ID).Find(&setList)
	if tx.Error != nil {
		return nil, tx.Error
	}

	setMap := make(map[string]SetInfo, setCount)
	var i int64
	for i = 0; i < setCount; i++ {
		setMap[setList[i].Name] = setList[i]
	}
	return setMap, nil
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

func parseDuplicateValue(errstr string) []string {
	s := strings.Index(errstr, "'") + 1
	e := strings.Index(errstr[s:len(errstr)-1], "'")
	e += s
	errstr = errstr[s:e]
	keys := strings.Split(errstr, "-")
	return []string{keys[0] + "-" + keys[1], keys[2], keys[3], keys[4]}
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
