#!/usr/bin/env python3
import json
import traceback

import requests

def socrata(view):
    is_href = view["viewType"] == "href"
    potential_links = view.get('metadata', {}).get('accessPoints', {}).values()
    links = filter(lambda x: '://' in x, potential_links)
    return {
        'is_link': is_href,
        'url': None if links == [] else links[0],
        'software': 'socrata',
        'identifier': view['tableId'],
    }

def ckan(dataset):
    try:
    # print(dataset['name'])
    # dataset['url']
    # all_links = [resource['url'] for resource in dataset['resources']]
        if dataset.get('resources', []) == []:
            current_link = None
        else:
            current_link = dataset['resources'][-1]['url']
    except:
        print dataset['name']
        raise
    return {
        'is_link': True,
        'url': current_link,
        'software': 'ckan',
        'identifier': dataset['name'],
    }

def is_alive(url):
    try:
        r = requests.head(url, allow_redirects=True)
    except:
        error = traceback.extract_stack()
        print('Failed:    ', url)
    else:
        error = None
        print('Succeeded: ', url)

    return r.status_code, json.dumps(dict(r.headers)), json.dumps(error)
