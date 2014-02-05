library(sqldf)
library(ggplot2)
library(reshape2)
library(scales)
library(knitr)
library(httr)

get.datasets <- function() {
  sql <- '
  -- CKAN
  SELECT 
    software, catalog, identifier, status_code, url
  FROM links
  WHERE software = \'ckan\'
  GROUP BY links.catalog, links.identifier

  UNION ALL

  -- Socrata
  SELECT 
    links.software, links.catalog, links.identifier, status_code, url
  FROM socrata_deduplicated
  JOIN links
  WHERE socrata_deduplicated.catalog = links.catalog
    AND socrata_deduplicated.tableId = links.identifier
  GROUP BY links.identifier
  ;'

  datasets <- with(new.env(), sqldf(sql, dbname = '/tmp/open-data.sqlite'))

  datasets$software <- factor(datasets$software)
  datasets$catalog <- factor(datasets$catalog)
  datasets$status_code <- factor(datasets$status_code)
  levels(datasets$status_code)[grep('-42', levels(datasets$status_code))] <- 'No response'
  datasets$status_code <- factor(datasets$status_code,
    levels = c(sort(levels(datasets$status_code)), NA), exclude = c())
  levels(datasets$status_code)[is.na(levels(datasets$status_code))] <- 'Not link'

  datasets
}

get.catalogs <- function(datasets) {
  catalogs <- sqldf('
SELECT
  software,
  catalog,
  sum(status_code == \'No response\') \'no_response\',
  sum(status_code == \'Not link\') \'not_links\',
  sum(status_code != \'Not link\' AND status_code != \'No response\' AND status_code = 200) \'live_links\',
  sum(status_code != \'Not link\' AND status_code != \'No response\' AND status_code != 200 AND status_code NOT NULL) \'dead_links\',
  count(*) \'datasets\'
FROM datasets GROUP BY catalog')
  catalogs$prop.bad <- catalogs$live_links / (catalogs$datasets - catalogs$not_links)
  catalogs$catalog <- factor(catalogs$catalog,
    levels = catalogs$catalog[order(catalogs$prop.bad, decreasing = TRUE)])
  catalogs
}

get.link.groupings <- function(catalogs) {
  for (column in c('no_response','not_links','live_links','dead_links')){
    catalogs[,column] <- catalogs[,column] / catalogs$datasets
  }

  link.groupings <- melt(catalogs,
    id.vars = c('software','catalog'),
    measure.vars = c('not_links','live_links','dead_links','no_response'),
    variable.name = 'link.type', value.name = c('proportion'))

  link.groupings <- link.groupings[order(link.groupings$catalog),]
  link.groupings$catalog <- factor(link.groupings$catalog,
    levels = levels(catalogs$catalog)[order(catalogs$prop.bad)])

  link.groupings
}


get.duplicates <- function() {
  sql = '
SELECT
  software, catalog, identifier, error,
  (min(status_code) = max(status_code)) same_status_code,
  count(*) n, round(avg(is_link)) is_link
FROM links
GROUP BY catalog, identifier
'
  unique.links <- with(new.env(), sqldf(sql, dbname = '/tmp/open-data.sqlite'))
  unique.links
}

get.errors <- function() {
  sql <- 'SELECT * FROM link_speeds'
  datasets <- with(new.env(), sqldf(sql, dbname = '/tmp/open-data.sqlite'))

  datasets$hostname <- sub('(?:(?:http|ftp|https)://)?([^/]*)/.*$', '\\1', datasets$url)

  datasets$error_type[is.na(datasets$error_type) | datasets$error_type == ''] <- 'No error'
  datasets$error_type <- sub("^<class 'requests.*exceptions.([^']*)'>", '\\1', datasets$error_type)
  datasets$error_type <- factor(datasets$error_type,
    levels = names(sort(table(errors$error_type), decreasing = TRUE)))

  datasets$error <- factor(datasets$error)

  datasets$hostname.pretty <- factor(datasets$hostname,
    levels = c(names(sort(table(datasets$hostname), decreasing = TRUE)[1:7]), NA), exclude = c())
  levels(datasets$hostname.pretty)[is.na(levels(datasets$hostname.pretty))] <- 'Other'

  datasets
}

if (!all(list('datasets', 'catalogs', 'unique.links', 'link.groupings') %in% ls())) {
  datasets <- get.datasets()
  datasets$has.scheme <- grepl('://', datasets$url) | grepl('^//', datasets$url)
  datasets$hostname <- sub('(?:(?:http|ftp|https)://)?([^/]*)/.*$', '\\1', datasets$url)
  catalogs <- get.catalogs(datasets)
  unique.links <- get.duplicates()
  link.groupings <- get.link.groupings(catalogs)
  errors <- get.errors()
}

p.codes <- ggplot(datasets) + aes(x = status_code) + geom_bar() +
  xlab('HTTP status code') +
  scale_y_continuous('Number of datasets', labels = comma) +
  ggtitle('Which status codes were returned when I checked link liveliness?')

p.data.openva.com <- ggplot(subset(datasets, catalog == 'data.openva.com')) +
  aes(x = status_code) + geom_bar() +
  xlab('HTTP status code') +
  scale_y_continuous('Number of datasets', labels = comma) +
  ggtitle('Which status codes were returned when I checked link liveliness on data.openva.com?')

p.dati.trentino.it <- ggplot(subset(datasets, catalog == 'dati.trentino.it')) +
  aes(x = status_code) + geom_bar() +
  xlab('HTTP status code') +
  scale_y_continuous('Number of datasets', labels = comma) +
  ggtitle('Which status codes were returned when I checked link liveliness on dati.trentino.it?')

p.bad <- ggplot(subset(catalogs, datasets > 0)) +
  aes(x = catalog, y = prop.bad, fill = software) +
  geom_bar(stat = 'identity') +
  xlab('Data catalog\n(Only data catalogs with externally stored datasets are included.)') +
  scale_y_continuous('Proportion of external datasets that did not respond', labels = percent) +
  ggtitle('Unresponsive external links by data catalog') +
  theme(legend.position = 'bottom') +
  coord_flip()

p.catalogs <- ggplot(catalogs) +
  aes(x = links, y = no_response, color = catalog == 'data.openva.com' | catalog == 'dati.trentino.it', label = catalog) +
  theme(legend.position = 'none') +
  scale_x_log10('Number of external links on the catalog', labels = comma, breaks = 10^(0:5)) +
  scale_y_log10('Number of non-responses when accessing external links', labels = comma, breaks = 10^(0:5)) +
  geom_text(size = 7, alpha = 0.5)

p.duplicates.ckan <- ggplot(subset(unique.links, software == 'ckan')) +
  aes(x = n) + facet_wrap(~ is_link, nrow = 2) + geom_histogram()

p.duplicates.socrata <- ggplot(subset(unique.links, software == 'socrata')) +
  aes(x = n) + facet_wrap(~ is_link, nrow = 2) + geom_histogram() +
  scale_y_sqrt()

unique.links.socrata <- subset(unique.links, software == 'socrata' & is_link)
table.duplicates.socrata <- table(subset(unique.links, software == 'socrata' & is_link)$n)
table.duplicates.socrata.by.catalog <- table(unique.links.socrata$catalog, unique.links.socrata$n)

p.link.types <- ggplot(link.groupings) +
  aes(x = catalog, y = proportion, fill = link.type) +
  geom_bar(stat = 'identity') + coord_flip() +
  xlab('') +
  scale_y_continuous('Proportion of datasets by catalog', labels = percent) +
  theme(legend.position = 'bottom', axis.text.y = element_text(size = 10)) +
  scale_fill_discrete('Type of dataset') +
  ggtitle('Non-links, live links and dead links across data catalogs')

p.link.types.specifics <- ggplot(subset(link.groupings, catalog == 'dati.trentino.it' | catalog == 'data.openva.com')) +
  aes(x = catalog, y = proportion, fill = link.type) +
  geom_bar(stat = 'identity') + coord_flip() +
  xlab('') +
  scale_y_continuous('Proportion of datasets by catalog', labels = percent) +
  theme(legend.position = 'bottom', axis.text.y = element_text(size = 10)) +
  scale_fill_discrete('Type of dataset') +
  ggtitle('Non-links, live links and dead links across data catalogs')

datasets$catalog <- factor(datasets$catalog,
  levels = sqldf('SELECT catalog from datasets group by catalog order by avg(has_scheme)')[,1])
datasets$has.scheme.factor <- factor(datasets$has.scheme, levels = c(TRUE, FALSE))
levels(datasets$has.scheme.factor) <- c('Yes','No')

p.scheme <- ggplot(subset(datasets, status_code != 'Not link')) +
  aes(x = catalog, fill = has.scheme) +
  theme(legend.position = 'bottom', axis.text.y = element_text(size = 10)) +
  xlab('') +
  scale_fill_discrete('Does the dataset have a URI scheme (like "http://")?') +
  coord_flip()

p.scheme.count <- p.scheme + geom_bar() +
  scale_y_continuous('Number of datasets', labels = comma)

p.scheme.prop <- p.scheme + geom_bar(position = 'fill') +
  scale_y_continuous('Proportion', labels = percent)

p.no_scheme <- ggplot(subset(datasets, !has.scheme & status_code != 'Not link')) +
  aes(x = catalog, fill = status_code) +
  geom_bar() +
  ggtitle('URIs without schemes wind up timing out.')

p.errors.total <- ggplot(errors) +
  aes(x = error_type) +
  geom_bar()

p.hostname.total <- ggplot(errors) +
  aes(x = hostname.pretty) +
  geom_bar() +
  xlab('"hostname" extracted my my hostname-extractor') +
  scale_y_continuous('Number of seemingly dead links', labels = comma) +
  ggtitle('Which servers give errors?')

p.hostname.error <- ggplot(errors) +
  aes(x = hostname.pretty, fill = error_type) +
  geom_bar() +
  xlab('"hostname" extracted my my hostname-extractor') +
  scale_y_continuous('Number of seemingly dead links', labels = comma) +
  scale_fill_discrete('Type of error') +
  ggtitle('Reason for dead link is related to the server serving the link.')

t.hostnames <- list()
for (.hostname in levels(errors$hostname.pretty)){
  t.hostnames[[.hostname]] <- subset(errors, hostname.pretty == .hostname)[1:5,c('error_type','url')]
}

t.error_types <- list()
for (.error_type in levels(errors$error_type)){
  t.error_types[[.error_type]] <- subset(errors, error_type == .error_type)[1:5,c('hostname','url')]
}

p.hostname.facet <- ggplot(subset(errors, error_type != 'InvalidURL')) +
  aes(x = error_type) + facet_wrap(~ hostname.pretty, ncol = 2) +
  geom_bar() +
  xlab('"hostname" extracted my my hostname-extractor') +
  scale_y_continuous('Number of seemingly dead links', labels = comma) +
  scale_fill_discrete('Type of error') +
  coord_flip() +
  ggtitle('Reason for dead link is related to the server serving the link.')

p.storage <- ggplot(errors) + aes(x = error_type, fill = grepl('/storage/', url)) + geom_bar()

p.elapsed <- ggplot(errors) +
# facet_wrap(~ error_type, scale = 'free', ncol = 1) +
# geom_bar(data = subset(errors, error_type == 'No error'), aes(x = hostname.pretty)) +
  geom_point(subset(errors, error_type == 'Timeout'), aes(x = hostname.pretty, y = elapsed))

#sqlite> select count(*), url like '% %' from links where is_link and url not null group by 2;

# table(datasets$catalog, datasets$has.scheme)

# knit('zombie-links.Rmd')
