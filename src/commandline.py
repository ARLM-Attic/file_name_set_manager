# -*- coding: utf-8 -*-
"""
commandline.py 

parse commandline
"""
import argparse

class CommandlineError(Exception):
    pass

_program_description = "maintain redis sets of of file names by watching a " \
                       "directory with inotify" 

def parse_commandline():
    """
    organize program arguments
    """
    parser = argparse.ArgumentParser(description=_program_description)
    parser.add_argument("-l", "--log", dest="log_path", help="/path/to/logfile")
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument("-w", "--watch", dest="watch_path",  
                       help="/path/to/watch/directory") 
    parser.add_argument("-k", "--key", dest="key_regex", 
                        help="regular expression that will identify the key" \
                        " from a file name")
    parser.add_argument("-p", "--prefix", dest="redis_prefix", 
                        help="prefix for key to construct the redis key")

    args = parser.parse_args()

    if args.log_path is None:
        parser.print_help()
        raise CommandlineError("You must specify a log path")

    if args.watch_path is None:
        parser.print_help()
        raise CommandlineError("You must specify a directory to watch")

    if args.key_regex is None:
        parser.print_help()
        raise CommandlineError("You must specify a regular expression to " \
                               "identify the key")

    if args.redis_prefix is None:
        parser.print_help()
        raise CommandlineError("You must specify a prefix for constructing " \
                               "the redis key")

    return args
