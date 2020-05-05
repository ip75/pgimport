# pgimport
import csv files to postgresql database

# install
    go get github.com/ip75/pgimport

# usage

    ./pgimport -fields="aoid","aoguid","parentguid" -table=object Objects.csv

```    
Usage of ./pgimport [OPTIONS] csv_file :
  -dbname string
        database name in pg (default "fias")
  -delimiter string
        delimiter in csv (default ";")
  -fields string
        table name where to import csv
  -host string
        database name in pg. (default "localhost")
  -pass string
        password to login to pg. (default "1")
  -port int
        pg port. (default 5432)
  -table string
        table name where to import csv
  -user string
        username to login to pg. (default "postgres")
 ```       
        
        
