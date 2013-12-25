download:
	python2 -c 'import run; run.download_metadata()'

plans.sqlite:
	python2 -c 'import run; run.plans()'
	
/tmp/deadlinks.sqlite:
	touch /tmp/deadlinks.sqlite
