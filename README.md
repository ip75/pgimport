# pgimport
import csv files to postgresql database

# install
go get github.com/ip75/pgimport

# usage

./pgimport -fields="aoid","aoguid","parentguid" -table=object Objects.csv
