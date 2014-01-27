library(sqldf)
library(ggplot2)
library(reshape2)
library(scales)
library(knitr)

get.datasets <- function() {
  sql <- '
  -- CKAN
  SELECT 
    software, catalog, identifier, status_code
  FROM links
  WHERE software = \'ckan\'

  UNION ALL

  -- Socrata
  SELECT 
    links.software, links.catalog, links.identifier, status_code
  FROM socrata_deduplicated
  JOIN links
  WHERE socrata_deduplicated.catalog = links.catalog
    AND socrata_deduplicated.tableId = links.identifier
  GROUP BY links.identifier
  ;'

  datasets <- with(new.env(), sqldf(sql, dbname = '/tmp/open-data.sqlite'))

  datasets$software <- factor(datasets$software)
  datasets$catalog <- factor(datasets$catalog)
  datasets$status_code <- factor(datasets$status_code, exclude = c())
  levels(datasets$status_code)[grep('-42', levels(datasets$status_code))] <- 'Timeout'
  levels(datasets$status_code)[is.na(levels(datasets$status_code))] <- 'Not link'

  datasets
}

if (!all(list('datasets') %in% ls())) {
  datasets <- get.datasets()
}

# knit('zombie-links.Rmd')
