CREATE VIEW IF NOT EXISTS socrata_dataset_count AS
SELECT catalog, count(*) 'datasets' FROM
  (
    SELECT catalog, tableId
    FROM socrata_deduplicated
    GROUP BY catalog, tableId
  )
GROUP BY catalog;
