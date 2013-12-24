#!/usr/bin/env python2
from time import sleep
import os, json
import csv

from get import get
from dumptruck import DumpTruck

import parallel
import download
import read
import links

SOCRATA_FIX = {
    'http://datakc.org':'https://opendata.go.ke',
    'www.data.gov': 'https://explore.data.gov',
    'www.consumerfinance.gov': None,
    'www.usaid.gov': None,
    'ethics.data.gov': None,
}
CKAN_FIX = {
}

def catalogs():
    for i in json.loads(get('http://instances.ckan.org/config/instances.json')):
        url = i['url']
        if url in CKAN_FIX:
            url = CKAN_FIX[url]
        if url != None:
            yield 'ckan', url

    for row in json.loads(get('https://opendata.socrata.com/api/views/6wk3-4ija/rows.json?accessType=DOWNLOAD'))['data']:
        url = list(filter(None, row[11]))[0]
        if url in SOCRATA_FIX:
            url = SOCRATA_FIX[url]
        if url != None:
            yield 'socrata', url

def download_metadata():
    import signal
    import sys
    from multiprocessing import Process

    processes = {}
    for software, url in catalogs():
        args = (url, os.path.join('downloads', software))
        processes[(software, url)] = Process(target = getattr(download, software), args = args)

    def signal_handler(signal, frame):
        parallel.kill(processes)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    parallel.start(processes)
    parallel.join(processes)

def check_links():
    dt = DumpTruck(dbname = '/tmp/links.sqlite', adapt_and_convert = False)
    dt.create_table({'identifier':'abcdef'}, if_not_exists = True)
    dt.create_index(['identifier'], if_not_exists = True)

    for catalog in read.catalogs('ckan'):
        for dataset in read.ckan(catalog):
            row = links.ckan(dataset)
            row.update({
                'catalog': catalog,
            })
            dt.insert(row)

    for catalog in read.catalogs('socrata'):
        for dataset in read.socrata(catalog):
            row = links.socrata(dataset)
            row.update({
                'catalog': catalog,
            })
            yield row

if __name__ == '__main__':
#   download_metadata()
#   exit()
    try:
        os.removedirs(os.path.join('downloads', 'ckan', 'datameti.go.jp', 'data'))
    except OSError:
        pass

    f = open('/tmp/links.csv')
    w = csv.DictWriter(f, fieldnames = [
        'software',
        'catalog',
        'identifier',
        'url',
        'is_link',
    ])
    for row in check_links():
        w.writerow(row)
