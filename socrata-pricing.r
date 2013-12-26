library(sqldf)

q <- function(sql) {
  sqldf(sql, dbname = '/tmp/open-data.sqlite')
}

q('SELECT 
