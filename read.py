def catalogs(software):
    return os.listdir(os.path.join('downloads', software))

def socrata(catalog):
    directory = os.path.join('downloads','socrata',catalog)
    for filename in os.listdir(directory):
        for view in json.load(open(os.path.join(directory, filename))):
            yield view

def ckan(catalog):
    directory = os.path.join('downloads','socrata',catalog)
    for filename in os.listdir(directory):
        yield json.load(open(os.path.join(directory, filename)))

