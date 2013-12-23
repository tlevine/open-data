#!/usr/bin/env python3
import json

def socrata(view):
    is_href = view["viewType"] == "href"
    links = view.get('metadata', {}).get('accessPoints', {}).values()
    return {
        'is_link': is_href,
        'url': links[0],
        'software': 'socrata',
        'identifier': view['tableId'],
        'is_alive': is_alive(links[0]),
    }

def ckan(dataset):
    # print(dataset['name'])
    # dataset['url']
    # all_links = [resource['url'] for resource in dataset['resources']]
    current_link = None if dataset['resources'] == [] else dataset['resources'][-1]['url']
    return {
        'is_link': True,
        'url': current_link,
        'software': 'ckan',
        'identifier': dataset['name'],
        'is_alive': is_alive(current_link),
    }

def is_alive(url):
    return True
