#!/usr/bin/env python
import os, re
import json
from lxml.html import parse

def parse_page(html):
    source = parse_source(html)
    targets = parse_targets(html)
    return [(source,target) for target in targets]

def parse_targets(html):
    srcs = html.xpath('//h4[text()="Federated Domains"]/following-sibling::ul[position()=1]/li/a/img/@src')
    return [unicode(re.match(r'/api/domains/([^/]+)/icons/smallIcon', src).group(1)) for src in srcs[1:]]

def parse_source(html):
    url = html.xpath('//meta[@property="og:url"]/@content')[0]
    return unicode(re.match(r'https://([^/]+)/browse/embed', url).group(1))

def build_network():
    htmls = [parse(os.path.join('homepages',homepage)).getroot() for homepage in os.listdir('homepages')]
    return {
        'edges': reduce(lambda a,b: a + parse_page(b), htmls, []),
        'nodes': map(parse_source, htmls),
    }

def identifiers(dcat):
    identifiers = Counter([dataset['identifier'] for dataset in dcat])
    return identifiers

def dedupe(dcat, edges):
    '''
    Args:
        An iterable of Socrata dcat, each list augmented with a "portal" key
    Returns:
        An iterable of dcat, still augmented with the "portal" key

    This deduplicates and combines of dcat based on the edges of the federation graph.
    '''

    losing_portals = set([edge[0] for edge in edges])
    duplicates = set((k for k,v in identifiers(dcat).iteritems() if v > 1))

    for dataset in dcat:
        if not(dataset['portal'] in losing_portals and dataset['identifier'] in duplicates):
            yield dataset

def main():
    edges = build_network()['edges']

    dt = DumpTruck(dbname = '/tmp/plans.sqlite', adapt_and_convert = True)
    quasi_dcat_in = dt.execute('SELECT * FROM datasets WHERE software = \'socrata\'')

    for dataset in quasi_dcat_in:
        dataset['identifier'] = dataset['id']

    quasi_dcat_out = list(dedupe(quasi_dcat_in, edges))

    for dataset in quasi_dcat_out:
        del dataset['identifier']

    dt.create_table(quasi_dcat_out, 'socrata_deduplicated')
    dt.create_index(['id'], 'socrata_deduplicated', if_not_exists = True, unique = True)
    dt.upsert(quasi_dcat_out)
