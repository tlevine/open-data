from urlparse import urljoin
import re
import os

import lxml.html
from get import get

def count_apis(catalog):
    raw = get(
        urljoin(catalog, '/browse?limitTo=apis&utf8=%E2%9C%93'),
        cachedir = os.path.join('downloads', 'socrata-apis'))
    html = lxml.html.fromstring(raw)
    resultCounts = html.xpath('//div[@class="resultCount"]/text()')
    if resultCounts == []:
        return 0
    else:
        m = re.match(r'^Showing ([0-9]+) of ([0-9]+)$', resultCounts[0].strip())
        return m.group(2)
