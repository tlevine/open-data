downloads/socrata-homepages:
	./socrata-federation-download.sh

download-dataset-metadata:
	python2 -c 'import run; run.download_metadata()'
	touch download-dataset-metadata

defederate: downloads/socrata-homepages datasets
	./socrata-federation-parse.py
	touch defederate

dataset-count: defederate
	sqlite3 /tmp/open-data.sqlite < views.sql
	dataset-count

datasets: download-dataset-metadata
	python2 -c 'import run; run.to_sqlite3()'
	touch datasets
	
get-links: datasets
	python2 -c 'import run; run.get_links()'
	touch get-links

check-links: get-links
	python2 -c 'import run; run.check_links()'
	touch check-links

apis:
	python2 -c 'import run; run.apis()'
	touch apis

reports/socrata-pricing.md: apis datasets dataset-count
	sqlite3 /tmp/open-data.sqlite < reports/socrata-pricing.sql
	cd reports && Rscript socrata-pricing.r

reports/dead-links.md: defederate check-links
	cd reports && Rscript dead-links.r

to-disk:
	mkdir -p cache
	cp /tmp/open-data.sqlite cache

from-disk:
	cp cache/open-data.sqlite /tmp
