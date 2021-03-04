SOURCE_FILES=tcgp.go utils.go models.go scraper.go
EXEC=scraper
$(EXEC) : 	$(SOURCE_FILES)
	go build -o scraper

clean :
	rm -v $(EXEC)

cleandb :
	go test --run=TestCleanUp