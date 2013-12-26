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

catalogs$has.forms <- factor(catalogs$n_forms > 0, levels = c(TRUE,FALSE))
catalogs$has.apis <- factor(catalogs$n_apis > 0, levels = c(TRUE,FALSE))
catalogs$has.311 <- factor(catalogs$n_311 > 0, levels = c(TRUE,FALSE))
levels(catalogs$has.forms) <-
  levels(catalogs$has.apis) <-
  levels(catalogs$has.311) <-
  c('Yes','No')

molten <- melt(catalogs, variable.name = 'has', value.name = 'yes',
  measure.vars = grep('has.', names(catalogs), value = TRUE))

p1 <- ggplot(catalogs) + aes(x = n_datasets, y = n_apis, color = n_forms > 0) +
  scale_x_log10('Number of datasets') + scale_y_log10('Number of APIs') +
  geom_point(size = 5)

p2 <- ggplot(molten) + aes(x = n_datasets, fill = yes) +
  facet_wrap(~ has) +
  geom_bar()

p3 <- ggplot(catalogs) + aes(x = n_forms, y = n_datasets, label = catalog) +
  facet_grid(has.apis ~ has.311) +
  scale_y_log10('Number of datasets') + scale_x_log10('Number of formss') +

  # Socrata Open Data Portal plans
  geom_hline(yintercept = c(150, 500, 1500), color = 'grey') + 

  # Socrata Data Collect plans
  geom_vline(xintercept = c(50, 100, 200), color = 'grey') + 

  geom_text()


t1 <- table(catalogs$has.forms, catalogs$has.apis, catalogs$has.311)

cat('To do: Guess the plans.\n')
