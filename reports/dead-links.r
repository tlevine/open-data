library(sqldf)
library(ggplot2)

get.datasets <- function() {
  sql <- '
  -- CKAN
  SELECT 
    software, catalog, identifier,
    is_link, status_code,
    coalesce(status_code == 200, NOT is_link) \'alive\'
  FROM links
  WHERE software = \'ckan\'

  UNION ALL

  -- Socrata
  SELECT 
    links.software, links.catalog, links.identifier,
    is_link, status_code,
    coalesce(status_code == 200, NOT is_link) \'alive\'
  FROM socrata_deduplicated
  JOIN links
  WHERE socrata_deduplicated.catalog = links.catalog
    AND socrata_deduplicated.tableId = links.identifier
  ;'

  datasets <- with(new.env(), sqldf(sql, dbname = '/tmp/open-data.sqlite'))

  datasets$software <- factor(datasets$software)
  datasets$catalog <- factor(datasets$catalog)
  datasets$alive.factor <- factor(datasets$alive, levels = 1:0)
  levels(datasets$alive.factor) <- c('Alive','Dead')
  datasets$status_code <- factor(datasets$status_code)
  datasets$status_code[datasets$status_code == '-42'] <- NA

  datasets
}

get.catalogs <- function(datasets) {
  catalogs <- sqldf('
  SELECT software, catalog, avg(alive) prop_alive
  FROM datasets
  GROUP BY catalog
  ')
  # Order by liveliness.
  catalogs$catalog <- factor(catalogs$catalog,
    levels = catalogs$catalog[order(catalogs$prop_alive)])

  catalogs
}

if (!('catalogs' %in% ls())) {
  datasets <- get.datasets()
  catalogs <- get.catalogs(datasets)
}

p.software <- ggplot(catalogs) +
  aes(x = catalog, y = prop_alive, fill = software) +
  geom_bar(stat = 'identity') + coord_flip()

p.socrata <- ggplot(datasets[datasets$software == 'socrata',]) +
  aes(x = mean(alive)) +
  facet_wrap(~ software) +
  geom_bar() + coord_flip()
