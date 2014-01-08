library(sqldf)
library(ggplot2)

sql <- '
SELECT 
  software, catalog, identifier,
  coalesce(status_code == 200, NOT is_link) \'alive\',
  status_code
FROM links;
'
datasets <- with(new.env(), sqldf(sql, dbname = '/tmp/open-data.sqlite'))
datasets$software <- factor(datasets$software)
datasets$catalog <- factor(datasets$catalog)
datasets$alive.factor <- factor(datasets$alive, levels = 1:0)
levels(datasets$alive.factor) <- c('Alive','Dead')
datasets$status_code <- factor(datasets$status_code)
datasets$status_code[datasets$status_code == '-42'] <- NA



p1 <- ggplot(datasets) +
  aes(x = catalog, y = mean(alive), fill = software) +
  geom_bar() + coord_flip()

p2 <- ggplot(datasets) +
  aes(x = catalog, fill = alive.factor) +
  facet_wrap(~ software) +
  geom_bar() + coord_flip()
