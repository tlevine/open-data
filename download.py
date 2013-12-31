'''
The directory variable is something like downloads/socrata, downloads/ckan, &c.
'''

import os
from time import sleep
import re
import json

from get import get

# ckanapi requires python 2
import ckanapi # https://twitter.com/CKANproject/status/378182161330753536
from ckanapi import  NotAuthorized, ValidationError

try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin

def remove_protocol(url):
    return re.sub(r'^https?://', '', url)

def ckan(url, directory):
    '''
    Args:
        portal: A string for the root of the portal (like "http://demo.ckan.org")
        directory: The directory to which stuff should be saved
    Returns:
        Nothing
    '''

    # Make sure the directory exists.
    try:
        os.makedirs(os.path.join(directory, remove_protocol(url)))
    except OSError:
        pass

    # Connect
    portal = ckanapi.RemoteCKAN(url)

    # Search
    try:
        datasets = portal.action.package_list()
    except:
        print('**Error searching %s**' % url)
        return

    # Metadata files
    for dataset in datasets:
        filename = os.path.join(directory, remove_protocol(url), dataset)
        if os.path.exists(filename):
            pass # print 'Already downloaded %s from %s' % (dataset, url_without_protocol)
        else:
            print('  Downloading %s from %s' % (dataset, url))
            try:
                dataset_information = portal.action.package_show(id = dataset)
            except NotAuthorized:
                print('**Not authorized for %s**' % url)
                return
            except ValidationError:
                print('**Validation error for %s**' % url)
                return

            fp = open(filename, 'w')
            json.dump(dataset_information, fp)
            fp.close()
            sleep(3)
    print('**Finished downloading %s**' % url)

def socrata(url, directory):
    page = 1
    while True:
        full_url = urljoin(url, '/api/views?page=%d' % page)
        filename = os.path.join(directory, re.sub('^https?://', '', full_url))
        raw = get(full_url, cachedir = directory)
        try:
            search_results = json.loads(raw)
        except ValueError:
            os.remove(filename)
            raw = get(full_url, cachedir = directory)
            try:
                search_results = json.loads(raw)
            except ValueError:
                print('**Something is wrong with %s**' % filename)
                break
        else:
            if len(search_results) == 0:
                break
        page += 1

def opendatasoft(url, directory):
    '''
    Args:
        url: A string for the root of the portal (like "http://demo.ckan.org")
        directory: The directory to which stuff should be saved
    Returns:
        Nothing
    '''
    # http://parisdata.opendatasoft.com/api/datasets/1.0/search?rows=1000000

    # Make sure the directory exists.
    try:
        os.makedirs(directory)
    except OSError:
        pass

    fn = os.path.join(directory, remove_protocol(url))
    if not os.path.exists(fn):
        try:
            get(url + '/api/datasets/1.0/search?rows=1000000', cachedir = directory)
        except:
            print('**Error downloading %s**' % url)
        else:
            print('  Downloaded %s' % url)

if __name__ == '__main__':
    ckan('http://datacatalogs.org', '/tmp')
