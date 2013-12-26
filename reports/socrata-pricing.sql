CREATE VIEW IF NOT EXISTS pricing_311 AS
SELECT catalog, count(*) 'n_311'
FROM socrata_deduplicated
WHERE name LIKE '%311%'
GROUP BY catalog;

CREATE VIEW IF NOT EXISTS pricing_geo AS
SELECT catalog, count(*) 'n_geo' FROM
(
  SELECT catalog FROM socrata_deduplicated
  WHERE viewType = 'geo'
  GROUP BY tableId
)
GROUP BY catalog;

CREATE VIEW IF NOT EXISTS pricing_forms AS
SELECT catalog, count(*) 'n_forms'
FROM socrata_deduplicated
WHERE displayType = 'form'
GROUP BY catalog;

CREATE VIEW IF NOT EXISTS pricing_form_tables AS
SELECT catalog, count(*) 'n_form_tables' FROM
(
  SELECT catalog FROM socrata_deduplicated
  WHERE displayType = 'form'
  GROUP BY tableId
)
GROUP BY catalog;

CREATE VIEW IF NOT EXISTS pricing_apis AS
SELECT * FROM (
  SELECT rtrim(catalog,'/') 'catalog', apis 'n_apis' FROM socrata_apis
  WHERE catalog NOT LIKE '%/%'
)
WHERE catalog NOT LIKE '%/%';
