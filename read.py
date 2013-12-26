import os, json

def catalogs(software):
    return os.listdir(os.path.join('downloads', software))

def socrata(catalog):
    directory = os.path.join('downloads','socrata',catalog,'api')
    if catalog != '.git':
        for filename in os.listdir(directory):
            for view in json.load(open(os.path.join(directory, filename))):
                yield view

def ckan(catalog):
    directory = os.path.join('downloads','ckan',catalog)
    if os.path.split(directory)[1] != '.git':
        for filename in os.listdir(directory):
            yield json.load(open(os.path.join(directory, filename)))

