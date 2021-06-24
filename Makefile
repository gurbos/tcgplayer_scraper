SOURCE_FILES=tcgp.go utils.go main.go
EXEC=scraper
GOPATH = $(shell go env GOPATH)

$(EXEC) : $(SOURCE_FILES)
	go build -ldflags "-w -s" -o ./bin/scraper

install : $(SOURCE_FILES)
	go build -ldflags "-w -s" -o $(GOPATH)/bin/scraper

clean :
	rm -v $(EXEC)

cleandb :
	go test --run=TestCleanUp

migrate: 
	go test --run=TestMigrate

