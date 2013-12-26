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

n.forms <- q('
SELECT catalog, count(*) \'n_forms\'
FROM socrata_deduplicated
WHERE displayType = \'form\'
GROUP BY catalog;
')

n.form.tables <- q('
SELECT catalog, count(*) \'n_form_tables\' FROM
(
  SELECT catalog FROM socrata_deduplicated
  WHERE displayType = \'form\'
  GROUP BY tableId
)
GROUP BY catalog;
')

n.apis <- q('SELECT * FROM socrata_apis WHERE catalog NOT LIKE \'%/%\';')
n.apis$catalog <- sub('/$', '', n.apis$catalog)
