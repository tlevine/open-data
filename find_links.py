def socrata(view):
    is_href = view["viewType"] == "href"
    links = view['metadata']['accessPoints'].values()
    return is_href, links

if __name__ == '__main__':
    import json
    for view in json.load(open('downloads/socrata/data.cityofchicago.org/api/views\?page\=1')):
        print(socrata(view))
