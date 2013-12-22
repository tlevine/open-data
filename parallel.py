#!/usr/bin/env python2
from multiprocessing import Process
import os

def create(portal_type, func, portal_urls):
    '''
    Args:
        portal_type: "ckan", "opendatasoft" or "socrata"
        func: function to download all datasets given a portal and directory
        portal_urls: List of portal domains
    Returns:
        A list of processes
    '''
    processes = {}

    for portal in portal_urls:
        args = (portal, os.path.join(ROOT_DIR, portal_type))
        processes[(portal_type, portal)] = Process(target = func, args = args)

    return processes

def start(processes):
    'Start all of the processes.'
    for process in processes.values():
        process.start()

def join(processes):
    'Wait for all of the processes to end.'
    for process in processes.values():
        process.join()

def kill(processes):
    'Kill all of the processes.'
    for process in processes.values():
        process.terminate()

def signal_handler(signal, frame):
    kill(processes)
    sys.exit(0)
