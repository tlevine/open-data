#!/usr/bin/env python
import os, re
import json
from collections import Counter
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

def homepages():
    directory = os.path.join('downloads', 'socrata-homepages', 'current')
    return (os.path.join(directory, filename) for filename in os.listdir(directory))

def build_network():
    htmls = [parse(homepage).getroot() for homepage in homepages()]
    return {
        'edges': reduce(lambda a,b: a + parse_page(b), htmls, []),
        'nodes': map(parse_source, htmls),
    }

def identifiers(datasets):
    identifiers = Counter([dataset['id'] for dataset in datasets])
    return identifiers

def dedupe(datasets, edges):
    '''
    Args:
        An iterable of Socrata dataset metadata, each list augmented with a "catalog" key
    Returns:
        An iterable of dataset metadata, still augmented with the "catalog" key

    This deduplicates and combines of dataset metadata based on the edges of the federation graph.
    '''

    losing_catalogs = set([edge[0] for edge in edges])
    duplicates = set((k for k,v in identifiers(datasets).iteritems() if v > 1))

    for dataset in datasets:
        if not(dataset['catalog'] in losing_catalogs and dataset['id'] in duplicates):
            yield dataset
