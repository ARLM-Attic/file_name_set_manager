# -*- coding: utf-8 -*-
"""
file_name_set_manager_main.py 

This program will maintain redis sets of of file names by watching a directory 
with inotify
"""
import logging
import os
import os.path

try:
    import queue
except ImportError:
    import Queue as queue

import sys
from threading import Event

from signal_handler import set_signal_handler
from log_setup import initialize_stderr_logging, initialize_file_logging
from commandline import parse_commandline, CommandlineError
from inotify_setup import create_notifier, create_notifier_thread, InotifyError

def _initial_directory_scan(watch_path, file_name_queue):    
    """
    load the queue with filenames found on startup
    """
    log = logging.getLogger("_initial_directory_scan")
    for file_name in os.listdir(watch_path):
        log.info("putting '{0}'".format(file_name))
        file_name_queue.put(file_name)

_dispatch_table = {}

def main():
    """
    main entry point for the program
    The return value will be the returncode for the program
    """
    initialize_stderr_logging()
    log = logging.getLogger("main")

    try:
        args = parse_commandline()
    except CommandlineError as instance:
        log.error("invalid commandline {0}".format(instance))
        return 1

    initialize_file_logging(args.log_path, args.verbose)

    halt_event = Event()
    set_signal_handler(halt_event)

    file_name_queue = queue.Queue()    

    try:
        notifier = create_notifier(args.watch_path, file_name_queue)
    except InotifyError as instance:
        log.error("Unable to initialize inotify: {0}".format(instance))
        return 1

    notifier_thread = create_notifier_thread(halt_event, notifier)
    notifier_thread.start()

    # we want the notifier running while we do the initial directory scan, so
    # we don't miss any files. But this leaves us open to duplicates()
    _initial_directory_scan(args.watch_path, file_name_queue)

    log.info("main loop starts")
    return_code = 0
    while not halt_event.is_set():

        try:
            file_name, event_name = file_name_queue.get(block=True, timeout=1.0)
        except queue.Empty:
            continue

        log.debug("found file_name '{0}' event {1}".format(file_name, 
                                                           event_name))
        try:
            _dispatch_table[event_name](file_name)
        except Exception:
            log.exception("{0} {1}".format(file_name, event_name))
            return_code = 1
            halt_event.set()

    log.info("main loop ends")
    notifier_thread.join(timeout=5.0)
#    assert not notifier_thread.is_alive
    notifier.stop()

    log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())