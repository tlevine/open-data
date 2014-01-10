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
            current_resource = {}
        else:
            current_resource = dataset['resources'][-1]
    except:
        print dataset['name']
        raise
    return {
        'is_link': current_resource.get('resource_type', 'file.upload') != 'file.upload',
        'url': current_resource.get('url'),
        'software': 'ckan',
        'identifier': dataset['name'],
    }

def is_alive(url):
    try:
        r = requests.head(url, allow_redirects=True, timeout = 2)
    except:
        status_code = -42
        headers = None
        error = traceback.extract_stack()
        print('Failed:    ' + url)
    else:
        status_code = r.status_code
        headers = dict(r.headers)
        headers['content-disposition'] = headers['content-disposition'].decode('unicode_escape')
        error = None
        print('Succeeded: ' + url)

    return status_code, json.dumps(headers), json.dumps(error)
