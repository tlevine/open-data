DROP VIEW IF EXISTS datasets_deduplicated;
CREATE VIEW datasets_deduplicated AS
SELECT
  datasets.software,
  datasets.catalog,
  datasets.identifier 'identifier',
  socrata_deduplicated.id 'slug'
FROM socrata_deduplicated
LEFT JOIN datasets
ON datasets.catalog = socrata_deduplicated.catalog AND
   datasets.identifier = socrata_deduplicated.id
UNION ALL
SELECT
  datasets.software,
  datasets.catalog,
  datasets.identifier 'identifier',
  datasets.identifier 'slug'
FROM datasets
WHERE software = 'ckan';

DROP VIEW IF EXISTS links_deduplicated;
CREATE VIEW IF NOT EXISTS links_deduplicated AS
SELECT * FROM links
GROUP BY links.catalog, links.identifier;
