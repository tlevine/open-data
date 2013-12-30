#!/usr/bin/env python2
from socrata_homepages import build_network, dedupe
from dumptruck import DumpTruck

def main():
    edges = build_network()['edges']

    dt = DumpTruck(dbname = '/tmp/open-data.sqlite', adapt_and_convert = True)
    datasets_in = dt.execute('SELECT * FROM socrata')

    dt.create_table({'id': 'blah-blah'}, 'socrata_deduplicated')
    dt.create_index(['id'], 'socrata_deduplicated', if_not_exists = True, unique = True)

    for dataset in dedupe(datasets_in, edges):
        dt.upsert(dataset, 'socrata_deduplicated')

if __name__== '__main__':
    main()
