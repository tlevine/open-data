library(sqldf)

q <- function(sql) {
  sqldf(sql, dbname = '/tmp/plans.sqlite')
}

q('SELECT 
