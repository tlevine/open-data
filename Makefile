downloads/socrata-homepages:
	./socrata-federation-download.sh

state/download-dataset-metadata: state
	python2 -c 'import run; run.download_metadata()'
	touch state/download-dataset-metadata

state/defederate: downloads/socrata-homepages state/datasets
	./socrata-federation-parse.py
	touch state/defederate

state/dataset-count: state/defederate
	sqlite3 /tmp/open-data.sqlite < views.sql
	state/dataset-count

state/datasets: state/download-dataset-metadata
	python2 -c 'import run; run.to_sqlite3()'
	touch state/datasets
	
state/check-links: state/datasets
	python2 -c 'import run; run.check_links()'
	touch state/check-links

state/apis: state
	python2 -c 'import run; run.apis()'
	touch state/apis

reports/socrata-pricing.md: state/apis state/datasets state/dataset-count
	sqlite3 /tmp/open-data.sqlite < reports/socrata-pricing.sql
	Rscript reports/socrata-pricing.r

to-disk:
	cp /tmp/open-data.sqlite cache

from-disk:
	cp cache/open-data.sqlite /tmp

state:
	mkdir -p state
