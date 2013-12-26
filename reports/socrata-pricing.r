library(sqldf)
library(ggplot2)
library(reshape2)

q <- function(sql) {
  sqldf(sql, dbname = '/tmp/open-data.sqlite')
}

catalogs <- q('SELECT * FROM pricing')
rownames(catalogs) <- catalogs$catalog
catalogs[-1] <- as.data.frame(lapply(catalogs[-1],as.integer))
# catalogs$catalog <- NULL
catalogs[is.na(catalogs)] <- 0

molten <- melt(catalogs, id.vars = c('catalog','n_datasets'))

p <- ggplot(molten) + aes(x = n_datasets, y = value, label = catalog) +
  scale_x_log10('Number of datasets') +
  scale_y_log10('Amount of the variable')
  facet_wrap(~ variable) + geom_point()
