download:
	python2 -c 'import run; run.download_metadata()'

defederate:
	./socrata-federation-download.sh

plans.sqlite:
	python2 -c 'import run; run.plans()'
	
/tmp/deadlinks.sqlite:
	touch /tmp/deadlinks.sqlite
