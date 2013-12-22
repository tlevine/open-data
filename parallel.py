#!/usr/bin/env python2
from multiprocessing import Process
import os

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
