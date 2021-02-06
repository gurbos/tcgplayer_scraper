package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
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

// TcgpError represents the errors returned by functions in the tcgp_scraper package.
type TcgpError struct {
	ErrorType   uint16
	ErrorCode   uint16
	ErrorString string
}

func (te *TcgpError) Error() string {
	return te.ErrorString
}

// ResponsePayload a structure used to hold values of a the decoded
// json string returned by a request to TcgPlayerDataURL.
type ResponsePayload struct {
	Errors  []string
	Results []Result
}

type Result struct {
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

// GetRequestPayload returns a RequestPayload object which identifies specific data. The fields of the
// RequestPayload object are encoded into JSON and sent as the paylaod of an http post request.
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

// tcgPlayerHTTPRequest returns an http.Request
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
