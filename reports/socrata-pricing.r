library(sqldf)

q <- function(sql) {
  sqldf(sql, dbname = '/tmp/open-data.sqlite')
}

n.311 <- q('
SELECT catalog, count(*) \'n_311\'
FROM socrata_deduplicated
WHERE name LIKE \'%311%\'
GROUP BY catalog
;')
