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
    'www.data.gov': None,
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

    try:
        os.removedirs(os.path.join('downloads', 'ckan', 'datameti.go.jp', 'data'))
    except OSError:
        pass

def _check_catalog(software, catalog):
    for dataset in getattr(read, software)(catalog):
        row = getattr(links, software)(dataset)
        row['catalog'] = catalog
        yield row

def datasets(softwares = ['ckan','socrata']):
    for software in softwares:
        for catalog in read.catalogs(software):
            if not SOCRATA_FIX.get(catalog, 'this is a string, not None') == None:
                for dataset in getattr(read, software)(catalog):
                    dataset['catalog'] = catalog
                    dataset['software'] = software
                    yield dataset

def check_links(softwares = ['ckan','socrata']):
    for software in softwares:
        for catalog in read.catalogs(software):
            if SOCRATA_FIX.get(catalog, 'this is a string, not None') == None:
                continue
            try:
                for row in _check_catalog(software, catalog):
                    print(row)
            except:
                print(os.path.join('downloads',software,catalog))
                raise

SOFTWARE_MAP = {
    'identifier': {'ckan':'name','socrata':'id'}
}
def plans():
    dt = DumpTruck('/tmp/plans.sqlite', adapt_and_convert = False, auto_commit = False)

    dummyrow = dict(zip(['software','catalog','identifier'], ['blah']*3))
    dt.create_table(dummyrow, 'datasets', if_not_exists = True)
    dt.create_index(['software','catalog','identifier'], 'datasets', if_not_exists = True, unique = True)

    dt.create_table({'view_id':'abc','table_id':123}, 'socrata_tables')
    dt.create_index(['view_id'], 'socrata_tables', if_not_exists = True, unique = True)
    dt.create_index(['table_id'], 'socrata_tables', if_not_exists = True)

    for dataset in datasets():
        row = {
            'software': dataset['software'],
            'catalog': dataset['catalog'],
            'identifier': dataset[SOFTWARE_MAP['identifier'][dataset['software']]],
        }
        dt.upsert(row, 'datasets')
        if dataset['software'] == 'socrata':
            socrata_table = {
                'view_id': row['identifier'],
                'table_id': dataset['tableId'],
            }
            dt.upsert(socrata_table, 'socrata_tables')
        dt.commit()
        print(row)
        break
