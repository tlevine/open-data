library(sqldf)
library(ggplot2)
library(reshape2)

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
  SELECT
    a.software, a.catalog,
    a.prop_links,
    a.prop_alive,
    a.n_datasets,
    b.prop_live_links
  FROM (
    SELECT
      software, catalog,
      avg(is_link) prop_links,
      avg(alive) prop_alive,
      count(*) n_datasets
    FROM datasets
    GROUP BY catalog
  ) a
  JOIN (
    SELECT
      software, catalog, avg(alive) prop_live_links
    FROM datasets
    WHERE is_link
    GROUP BY catalog
  ) b
  WHERE a.catalog = b.catalog
  ')

  # Order by liveliness.
  catalogs$catalog <- factor(catalogs$catalog,
    levels = catalogs$catalog[order(catalogs$prop_alive)])

  catalogs$has_links <- factor(catalogs$prop_links > 0, levels = c(TRUE, FALSE))
  levels(catalogs$has_links) <- c('Yes','No')

  catalogs
}

get.link.groupings <- function(catalogs) {
  catalogs$not.links <- 1 - catalogs$prop_links
  catalogs$live.links <- catalogs$prop_links * catalogs$prop_live_links
  catalogs$dead.links <- catalogs$prop_links * (1 - catalogs$prop_live_links)
  
  link.groupings <- melt(catalogs,
    id.vars = c('software','catalog'),
    measure.vars = paste(c('not', 'live', 'dead'), 'links', sep = '.'),
    variable.name = 'link.type', value.name = c('proportion'))

  link.groupings
}

if (!('catalogs' %in% ls())) {
  datasets <- get.datasets()
  catalogs <- get.catalogs(datasets)
  link.groupings <- get.link.groupings(catalogs)
}


p.has_links <- qplot(data = catalogs, x = software, fill = has_links,
  position = 'fill', geom = 'bar')

p.software <- ggplot(catalogs) +
  aes(x = catalog, y = prop_alive, fill = software) +
  geom_bar(stat = 'identity') + coord_flip()

p.prop_links <- ggplot(catalogs) +
  aes(y = prop_links, x = prop_alive, color = software) +
  geom_point() + coord_flip()

p.software.only_links <- ggplot(catalogs) +
  aes(x = catalog, y = prop_live_links, fill = software) +
  geom_bar(stat = 'identity') + coord_flip()
