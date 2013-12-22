import json

from get import get

# ckanapi requires python 2
import ckanapi # https://twitter.com/CKANproject/status/378182161330753536
import ckan.logic

try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin

def ckan(url, directory):
    '''
    Args:
        portal: A string for the root of the portal (like "http://demo.ckan.org")
        directory: The directory to which stuff should be saved
    Returns:
        Nothing
    '''
    url_without_protocol = re.sub(r'^https?://', '', url)

    # Make sure the directory exists.
    try:
        os.makedirs(os.path.join(directory, url_without_protocol))
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
        filename = os.path.join(directory, url_without_protocol, dataset)
        if os.path.exists(filename):
            pass # print 'Already downloaded %s from %s' % (dataset, url_without_protocol)
        else:
            print('  Downloading %s from %s' % (dataset, url))
            try:
                dataset_information = portal.action.package_show(id = dataset)
            except ckan.logic.NotAuthorized:
                print('**Not authorized for %s**' % url)
                return
            fp = open(filename, 'w')
            json.dump(dataset_information, fp)
            fp.close()
            sleep(3)
    print('**Finished downloading %s**' % url)

def socrata(url, directory):
    page = 1
    while True:
        search_results = json.loads(get(urljoin(url, '/api/views?page=%d' % page)))
        if len(search_results) == 0:
            break
        page += 1
