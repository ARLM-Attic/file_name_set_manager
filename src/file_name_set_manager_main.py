# -*- coding: utf-8 -*-
"""
file_name_set_manager_main.py 

This program will maintain redis sets of of file names by watching a directory 
with inotify
"""
import logging
import os
import os.path
import re
try:
    import queue
except ImportError:
    import Queue as queue
import sys
from threading import Event
import time

from signal_handler import set_signal_handler
from log_setup import initialize_stderr_logging, initialize_file_logging
from commandline import parse_commandline, CommandlineError
from inotify_setup import create_notifier, create_notifier_thread, InotifyError
from redis_connection import create_redis_connection
from event_names import found_at_startup, \
                        directory_scan_finished, \
                        inotify_close_write, \
                        inotify_moved_to, \
                        inotify_delete, \
                        inotify_moved_from, \
                        inotify_idle

_up_to_date_timestamp = "up_to_date"

def _initial_directory_scan(watch_path, file_name_queue):    
    """
    load the queue with filenames found on startup
    """
    log = logging.getLogger("_initial_directory_scan")
    for file_name in os.listdir(watch_path):
        log.info("putting ({0}, {1})".format(file_name, found_at_startup))
        file_name_queue.put((file_name, found_at_startup, ))

    # mark the queue to show the end of these files
    file_name_queue.put((None, directory_scan_finished, ))
    log.debug("putting ({0}, {1})".format(None, directory_scan_finished))
 
def _process_incoming_file(redis, redis_key, file_name):
    log = logging.getLogger("_process_incoming_file")

    add_count = redis.sadd(redis_key, file_name)
    # this is probably some form of duplicate, so we don't abort
    if add_count == 0:
        log.warn("sadd({0}, {1}) returned add count 0".format(redis_key, 
                                                              file_name))
def _process_outgoing_file(redis, redis_key, file_name):
    # we don't warn on a zero count here, because the user can
    # delete the key when he is no longer interested in it
    _ = redis.srem(redis_key, file_name)

_dispatch_table = {found_at_startup        : _process_incoming_file,
                   inotify_close_write     : _process_incoming_file,
                   inotify_moved_to        : _process_incoming_file,
                   inotify_delete          : _process_outgoing_file,
                   inotify_moved_from      : _process_outgoing_file}

def main():
    """
    main entry point for the program
    The return value will be the returncode for the program
    """
    initialize_stderr_logging()
    log = logging.getLogger("main")

    try:
        args = parse_commandline()
    except CommandlineError:
        instance = sys.exc_info()[1]
        log.error("invalid commandline {0}".format(instance))
        return 1

    if not "(?P<key>" in args.key_regex:
        log.error("invalid key regex {0}".format(args.key_regex))
        return 1

    try:
        key_regex = re.compile(args.key_regex)
    except Exception:
        instance = sys.exc_info()[1]
        log.error("Unable to compile key_regex {0} {1}".format(args.key_regex,
                                                               instance))
        return 1        

    initialize_file_logging(args.log_path, args.verbose)

    log.info("key regex pattern = '{0}'".format(key_regex.pattern))

    halt_event = Event()
    set_signal_handler(halt_event)

    file_name_queue = queue.Queue()    

    try:
        notifier = create_notifier(args.watch_path, file_name_queue)
    except InotifyError as instance:
        log.error("Unable to initialize inotify: {0}".format(instance))
        return 1

    try:
        redis = create_redis_connection()
    except Exception:
        log.exception("Unable to connect to redis")
        return 1

    # clear REDIS of all keys under our namespace, so that old sets that 
    # don't have any files anymore don't stay around
    existing_keys = redis.keys("_".join([args.redis_prefix, "*"]))
    if len(existing_keys) > 0: 
        log.debug("deleting {0} existing REDIS keys".format(
                  len(existing_keys), ))
        redis.delete(*existing_keys)

    # set our up-to-date time as far back as we can: we aren't up to date yet
    up_to_date_timestamp_key = "_".join([args.redis_prefix, 
                                         _up_to_date_timestamp])
    log.debug("setting {0} to {1}".format(up_to_date_timestamp_key, 0))
    redis.set(up_to_date_timestamp_key, "0")

    notifier_thread = create_notifier_thread(halt_event, 
                                             notifier, 
                                             file_name_queue)

    # we want the notifier running while we do the initial directory scan, so
    # we don't miss any files. 
    notifier_thread.start()

    _initial_directory_scan(args.watch_path, file_name_queue)

    log.info("main loop starts")
    directory_scan_up_to_date = False
    up_to_date = False
    return_code = 0
    while not halt_event.is_set():

        try:
            file_name, event_name = file_name_queue.get(block=True, timeout=1.0)
        except queue.Empty:
            continue
        except KeyboardInterrupt:
            log.warn("KeyboardInterrupt: halting")
            halt_event.set()
            break

        if event_name == directory_scan_finished:
            log.debug("setting directory_scan_up_to_date")
            directory_scan_up_to_date = True
            continue

        if event_name == inotify_idle:
            if not up_to_date and directory_scan_up_to_date:
                current_time = int(time.time())
                log.debug("setting {0} to {1}".format(up_to_date_timestamp_key, 
                                                      current_time))
                redis.set(up_to_date_timestamp_key, str(current_time))
                up_to_date = True
            continue

        match_object = key_regex.match(file_name)
        if match_object is None:
            log.debug("unmatched file name '{0}'".format(file_name))
            continue
        key = match_object.group("key")

        log.debug("found file_name '{0}' key {1} event {2}".format(file_name, 
                                                                   key,
                                                                   event_name))
        redis_key = "_".join([args.redis_prefix, key, ])
        try:
            _dispatch_table[event_name](redis, redis_key, file_name)
        except Exception:
            log.exception("{0} {1}".format(file_name, event_name))
            return_code = 1
            halt_event.set()

    log.info("main loop ends")
    redis.set(up_to_date_timestamp_key, "0")
    notifier_thread.join(timeout=5.0)
#    assert not notifier_thread.is_alive
    notifier.stop()

    log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())