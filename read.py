import os, json

def catalogs(software):
    return os.listdir(os.path.join('downloads', software))

def socrata(catalog):
    directory = os.path.join('downloads','socrata',catalog,'api')
    for filename in os.listdir(directory):
        if filename == '.git':
            continue
        for view in json.load(open(os.path.join(directory, filename))):
            yield view

def ckan(catalog):
    directory = os.path.join('downloads','ckan',catalog)
    for filename in os.listdir(directory):
        if filename == '.git':
            continue
        yield json.load(open(os.path.join(directory, filename)))

