#!/usr/bin/env python3
import json

def socrata(view):
    is_href = view["viewType"] == "href"
    links = view.get('metadata', {}).get('accessPoints', {}).values()
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
    return True
