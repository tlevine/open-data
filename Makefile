download:
	python2 -c 'import run; run.download_metadata()'

defederate: downloads/socrata-homepages open-data.sqlite
	./socrata-federation-parse.py

dataset_count: defederate
	sqlite3 /tmp/open-data.sqlite < views.sql

downloads/socrata-homepages:
	./socrata-federation-download.sh
	ln -s /tmp/socrata-defederate.sqlite socrata-defederate.sqlite

open-data.sqlite:
	python2 -c 'import run; run.to_sqlite3()'
	
dead-links.sqlite:
	touch /tmp/dead-links.sqlite

apis:
	python2 -c 'import run; run.apis()'

reports/socrata-pricing.md:
	sqlite3 /tmp/open-data.sqlite < reports/socrata-pricing.sql
	Rscript reports/socrata-pricing.r

to-disk:
	cp /tmp/open-data.sqlite cache

from-disk:
	cp cache/open-data.sqlite /tmp
