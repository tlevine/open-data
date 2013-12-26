library(sqldf)

q <- function(sql) {
  sqldf(sql, dbname = '/tmp/open-data.sqlite')
}

catalogs <- q('SELECT * FROM pricing')
rownames(catalogs) <- catalogs$catalog
catalogs[-1] <- as.data.frame(lapply(catalogs[-1],as.integer))
catalogs$catalog <- NULL
catalogs[is.na(catalogs)] <- 0
