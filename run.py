#!/usr/bin/env python2
from time import sleep
import os, json
import csv
import random

from get import get
from dumptruck import DumpTruck

import parallel
import download
import read
import links
from apis import count_apis

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

def get_links(softwares = ['ckan','socrata']):
    dt = DumpTruck('/tmp/open-data.sqlite')

    dummyrow = dict(zip(['software','catalog','identifier', 'error', 'status_code', 'headers'], (['blah'] * 4) + ([234] * 1) + [{'a':'b'}]))
    dt.create_table(dummyrow, 'links', if_not_exists = True)
    dt.create_index(['software','catalog','identifier'], 'links', if_not_exists = True, unique = True)

    for software in softwares:
        for catalog in read.catalogs(software):
            if SOCRATA_FIX.get(catalog, 'this is a string, not None') == None:
                continue
            try:
                for row in _check_catalog(software, catalog):
                    dt.upsert(row, 'links')
            except:
                print(os.path.join('downloads',software,catalog))
                raise

def check_links():
    dt = DumpTruck('/tmp/open-data.sqlite')
    dt.create_index(['url'], 'links', if_not_exists = True, unique = False)
    dt.create_index(['status_code'], 'links', if_not_exists = True, unique = False)
    urls = [row['url'] for row in dt.execute('SELECT DISTINCT url FROM links WHERE status_code IS NULL')]
    random.shuffle(urls) # so that we randomly bounce around catalogs
    for url in urls:
        status_code, headers = links.is_alive(url)
        sql = 'UPDATE links SET status_code = ?, headers = ? WHERE is_link = 1 AND url = ?'
        dt.execute(sql, (status_code, json.dumps(dict(headers)), url))

SOFTWARE_MAP = {
    'identifier': {'ckan':'name','socrata':'id'}
}
def to_sqlite3():
    dt = DumpTruck('/tmp/open-data.sqlite', auto_commit = False)

    dummyrow = dict(zip(['software','catalog','identifier'], ['blah']*3))
    dt.create_table(dummyrow, 'datasets', if_not_exists = True)
    dt.create_index(['software','catalog','identifier'], 'datasets', if_not_exists = True, unique = True)

    for table in ['ckan','socrata']:
        dt.create_table({'catalog':'blah','identifier':'blah'}, table, if_not_exists = True)
        dt.create_index(['catalog','identifier'], table, if_not_exists = True, unique = True)

    dt.create_table({'view_id':'abc','table_id':123}, 'socrata_tables')
    dt.create_index(['view_id'], 'socrata_tables', if_not_exists = True, unique = True)
    dt.create_index(['table_id'], 'socrata_tables', if_not_exists = True)

    for dataset in datasets():
        row = {
            'software': dataset['software'],
            'catalog': dataset['catalog'],
            'identifier': dataset[SOFTWARE_MAP['identifier'][dataset['software']]],
        }
        sql = 'SELECT * FROM datasets WHERE software = ? AND catalog = ? AND identifier = ?'
        if dt.execute(sql, [row['software'],row['catalog'],row['identifier']]) != []:
            continue
        dt.upsert(row, 'datasets')
        if dataset['software'] == 'socrata':
            socrata_table = {
                'view_id': row['identifier'],
                'table_id': dataset['tableId'],
            }
            dt.upsert(socrata_table, 'socrata_tables')
        dt.upsert(dataset,dataset['software'])
        dt.commit()

def apis():
    dt = DumpTruck('/tmp/open-data.sqlite', auto_commit = False)
    dt.create_table({'catalog':'abc.def'}, 'socrata_apis')
    dt.create_index(['catalog'], 'socrata_apis', unique = True, if_not_exists = True)

    socrata_catalogs = filter(lambda x: x[0] == 'socrata', catalogs())
    for _, catalog in socrata_catalogs:
        dt.upsert({
            'catalog': catalog.split('://')[-1],
            'apis': count_apis(catalog),
        }, 'socrata_apis')

def fix_things():
    'Always run these.'
    try:
        os.removedirs(os.path.join('downloads', 'ckan', 'datameti.go.jp', 'data'))
    except OSError:
        pass

fix_things()
