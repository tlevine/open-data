DROP VIEW IF EXISTS pricing_311;
DROP VIEW IF EXISTS pricing_geo;
DROP VIEW IF EXISTS pricing_forms;
DROP VIEW IF EXISTS pricing_form_tables;
DROP VIEW IF EXISTS pricing_apis;
DROP VIEW IF EXISTS pricing;

CREATE VIEW pricing_311 AS
SELECT catalog, count(*) 'n_311'
FROM socrata_deduplicated
WHERE name LIKE '%311%'
GROUP BY catalog;

CREATE VIEW pricing_geo AS
SELECT catalog, count(*) 'n_geo' FROM
(
  SELECT catalog FROM socrata_deduplicated
  WHERE viewType = 'geo'
  GROUP BY tableId
)
GROUP BY catalog;

CREATE VIEW pricing_forms AS
SELECT catalog, count(*) 'n_forms'
FROM socrata_deduplicated
WHERE displayType = 'form'
GROUP BY catalog;

CREATE VIEW pricing_form_tables AS
SELECT catalog, count(*) 'n_form_tables' FROM
(
  SELECT catalog FROM socrata_deduplicated
  WHERE displayType = 'form'
  GROUP BY tableId
)
GROUP BY catalog;

CREATE VIEW pricing_apis AS
SELECT * FROM (
  SELECT rtrim(catalog,'/') 'catalog', apis 'n_apis' FROM socrata_apis
  WHERE catalog NOT LIKE '%/%'
)
WHERE catalog NOT LIKE '%/%';

CREATE VIEW pricing AS
SELECT
  socrata_dataset_count.catalog,
  socrata_dataset_count.datasets 'n_datasets',
  n_geo,
  n_311,
  n_forms,
  n_form_tables,
  n_apis
FROM socrata_dataset_count
LEFT JOIN pricing_geo ON socrata_dataset_count.catalog = pricing_geo.catalog
LEFT JOIN pricing_311 ON socrata_dataset_count.catalog = pricing_311.catalog
LEFT JOIN pricing_forms ON socrata_dataset_count.catalog = pricing_forms.catalog
LEFT JOIN pricing_form_tables ON socrata_dataset_count.catalog = pricing_form_tables.catalog
LEFT JOIN pricing_apis ON socrata_dataset_count.catalog = pricing_apis.catalog;
