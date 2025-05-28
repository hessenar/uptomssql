# uptomssql

Help:  
* -c string  
initial catalog (default "master")  
* -d string  
path to dir with data to upload (default "test_data")  
* -p string  
user password (default "test")  
* -s string  
db data source (default "localhost,1433")  
* -u string  
user id (default "test")

Return codes:
* 0 => success
* 1 => error on connect to db
* 2 => error on get table info
* 3 => error on data insert in table
* 4 => error on unmarshal inserted data
* 5 => error on read dir
* 6 => error on read file
* 7 => error on open file


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### Third-Party Libraries

This project uses the following third-party libraries:
- [sqlx](https://github.com/jmoiron/sqlx) - MIT License
- [go-mssqldb](https://github.com/microsoft/go-mssqldb) - BSD-3-Clause license
