library(sqldf)

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
datasets$alive <- as.logical(datasets$alive)
datasets$status_code <- factor(datasets$status_code)
