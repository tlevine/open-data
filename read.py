import os, json

def catalogs(software):
    return os.listdir(os.path.join('downloads', software))

WEIRD = {
    'data.sfgov.org':os.path.join('data.sfgov.org','api'),
}
def socrata(catalog):
    if catalog in WEIRD:
        catalog = WEIRD[catalog]
    directory = os.path.join('downloads','socrata',catalog)
    for filename in os.listdir(directory):
        for view in json.load(open(os.path.join(directory, filename))):
            yield view

def ckan(catalog):
    directory = os.path.join('downloads','ckan',catalog)
    for filename in os.listdir(directory):
        yield json.load(open(os.path.join(directory, filename)))

