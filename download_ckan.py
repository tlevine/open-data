#!/usr/bin/env python
from time import sleep
import os, json

from get import get
import ckanapi # https://twitter.com/CKANproject/status/378182161330753536

instances = [i['url'] for i in json.loads(get('http://instances.ckan.org/config/instances.json'))]

def download(portal_url, directory):
    '''
    Args:
        portal: A string for the root of the portal (like "http://demo.ckan.org")
        directory: The directory to which stuff should be saved
    Returns:
        Nothing
    '''

    # Make sure the directory exists.
    try:
        os.makedirs(directory)
    except OSError:
        pass

    portal = ckanapi.RemoteCKAN("http://" + portal_url)

    try:
        datasets = portal.action.package_list()
    except:
        print('**Error searching %s**' % portal_url)
        return

    for dataset in datasets:
        filename = os.path.join(directory, portal_url, dataset)
        if os.path.exists(filename):
            pass # print 'Already downloaded %s from %s' % (dataset, portal_url)
        else:
            print('  Downloading %s from %s' % (dataset, portal_url))
            dataset_information = portal.action.package_show(id = dataset)
            fp = open(filename, 'w')
            json.dump(dataset_information, fp)
            fp.close()
            sleep(3)
    print('**Finished downloading %s**' % portal_url)
