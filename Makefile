all: windows linux


freebsd:
	GOOS=freebsd
	GOARCH=amd64
	go build -o pgimport.freebsd

linux:
	GOOS=linux
	GOARCH=amd64
	go build -o pgimport

windows:
	GOOS=windows
	GOARCH=amd64
	go build
