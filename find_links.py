def socrata(view):
    is_href = view["viewType"] == "href"
    links = view.get('metadata', {}).get('accessPoints', {}).values()
    return is_href, links

def ckan(dataset):
    # current_link = dataset['resources'][-1]['url']
    all_links = [resource['url'] for resource in dataset['resources']]
    return is_href, all_links

if __name__ == '__main__':
    import json
    for view in json.load(open('downloads/socrata/data.cityofchicago.org/api/views?page=1')):
        is_href, links = socrata(view)
        if is_href:
            print(links)
