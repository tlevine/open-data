library(sqldf)

q <- function(sql) {
  sqldf(sql, dbname = '/tmp/open-data.sqlite')
}

n.datasets <- q('SELECT * FROM socrata_dataset_count')

n.311 <- q('
SELECT catalog, count(*) \'n_311\'
FROM socrata_deduplicated
WHERE name LIKE \'%311%\'
GROUP BY catalog
;')

n.geospatial <- q('
SELECT catalog, count(*) \'n_geo\' FROM
(
  SELECT catalog FROM socrata_deduplicated
  WHERE viewType = \'geo\'
  GROUP BY tableId
)
GROUP BY catalog;
')
