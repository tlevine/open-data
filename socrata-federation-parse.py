#!/usr/bin/env python
from socrata_homepages import build_network, dedupe
from dumptruck import DumpTruck

def main():
    edges = build_network()['edges']

    dt = DumpTruck(dbname = '/tmp/open-data.sqlite', adapt_and_convert = True)
    datasets_in = dt.execute('SELECT * FROM datasets WHERE software = \'socrata\'')
    datasets_out = list(dedupe(datasets_in, edges))

    dt.create_table(datasets_out, 'socrata_deduplicated')
    dt.create_index(['id'], 'socrata_deduplicated', if_not_exists = True, unique = True)
    dt.upsert(datasets_out)
