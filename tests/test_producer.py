# -*- coding: utf-8 -*-
"""
test_producer.py 

A program to feed files to file_name_set_manager
"""
import argparse
import logging
import os.path
import random
import sys
import time

class CommandlineError(Exception):
    pass

_program_description = "feed test files to watch directory" 

_log_format_template = "%(asctime)s %(levelname)-8s %(name)-20s: %(message)s"
_user_ids = [1001, 2001, 3001, 4001, 5001]
_device_ids = [1, 2, 3]
_low_xact_id = 1000
_high_xact_id = 10000

def _initialize_stderr_logging():
    """
    log errors to stderr
    """                                                   
    log_level = logging.DEBUG                                                    
    handler = logging.StreamHandler(stream=sys.stderr)                          
    formatter = logging.Formatter(_log_format_template)                         
    handler.setFormatter(formatter)                                             
    handler.setLevel(log_level)                                                 
    logging.root.addHandler(handler)                      

    logging.root.setLevel(log_level)

def _parse_commandline():
    """
    organize program arguments
    """
    parser = argparse.ArgumentParser(description=_program_description)
    parser.add_argument("-w", "--watch", dest="watch_path",  
                       help="/path/to/watch/directory") 

    parser.add_argument("--d", 
                        "--duration", 
                        dest="test_duration",
                        type=int,
                        default=60,
                        help="the duration the test in seconds")
    parser.add_argument("--min-files-per-key", 
                        dest="min_files_per_key",
                        type=int,
                        default=1,
                        help="lower bound of the number of files per key")
    parser.add_argument("--max-files-per-key", 
                        dest="max_files_per_key",
                        type=int,
                        default=100,
                        help="upper bound of the number of files per key")
    parser.add_argument("--min-key-interval", 
                        dest="min_key_interval",
                        type=float,
                        default=0.5,
                        help="minimum time (secs) to wait between keys")
    parser.add_argument("--max-key-interval", 
                        dest="max_key_interval",
                        type=float,
                        default=3.0,
                        help="maximum time (secs) to wait between keys")
    args = parser.parse_args()

    if args.watch_path is None:
        parser.print_help()
        raise CommandlineError("You must specify a directory to watch")

    return args

def _construct_random_key():
    return "{0}-{1}-{2}".format(random.choice(_user_ids),
                                random.choice(_device_ids),
                                random.randint(_low_xact_id, _high_xact_id))

def _store_one_key(args):
    """
    store a random number of files for a single key
    we construct the key in the form of a SpiderOak xact_log_id, which
    is what we use the file_name_set_manager program for.
    """
    key = _construct_random_key()
    file_count = random.randint(args.min_files_per_key,
                                args.max_files_per_key)
    for i in range(file_count):
        file_name = "{0}_{1:08}".format(key, i+1)
        file_path = os.path.join(args.watch_path, file_name)
        with open(file_path, "wb") as output_file:
            output_file.write(b"x")

def main():
    """
    main entry point for the program
    The return value will be the returncode for the program
    """
    _initialize_stderr_logging()
    log = logging.getLogger("main")
    log.info("program starts")

    args = _parse_commandline()

    start_time = time.time()
    elapsed_time = 0.0
    while elapsed_time < args.test_duration:
        try:
            _store_one_key(args)
        except Exception:
            log.exception("_store_one_key")
            return 1 
        interval = random.uniform(args.min_key_interval, args.max_key_interval)
        time.sleep(interval)
        elapsed_time = time.time() - start_time

    log.info("program completes return code = 0")
    return 0

if __name__ == "__main__":
    sys.exit(main())