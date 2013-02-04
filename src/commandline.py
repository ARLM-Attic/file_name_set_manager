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

    args = parser.parse_args()

    if args.log_path is None:
        parser.print_help()
        raise CommandlineError("You must specify a log path")

    if args.watch_path is None:
        parser.print_help()
        raise CommandlineError("You must specify a directory to watch")

    return args
