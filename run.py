#!/usr/bin/env python2
from time import sleep
import os, json
import re

from get import get

import parallel
from download import ckan

def main():
    import signal
    import sys
    from multiprocessing import Process

    urls = [i['url'] for i in json.loads(get('http://instances.ckan.org/config/instances.json'))]

    processes = {}
    for url in urls:
        args = (url, os.path.join('downloads','ckan'))
        processes[url] = Process(target = ckan, args = args)

    signal.signal(signal.SIGINT, parallel.signal_handler)
    parallel.start(processes)
    parallel.join(processes)

if __name__ == '__main__':
    main()
