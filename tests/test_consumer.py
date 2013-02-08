# -*- coding: utf-8 -*-
"""
test_consumer.py 

A program to consume files listed in file_name_set_manager
"""
import argparse
import logging
import os
import os.path
import random
import sys
import time

import redis

class CommandlineError(Exception):
    pass

_program_description = "consume test files from watch directory" 

_log_format_template = "%(asctime)s %(levelname)-8s %(name)-20s: %(message)s"
_redis_host = os.environ.get("REDIS_HOST", "localhost")
_redis_port = int(os.environ.get("REDIS_PORT", str(6379)))
_redis_db = int(os.environ.get("REDIS_DB", str(0)))

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
    parser.add_argument("-n", "--notification-channel", 
                        dest="notification_channel",  
                        default="file-name-set-manager-test-channel",
                        help="redis pub/sub channel for key notification") 
    args = parser.parse_args()

    if args.watch_path is None:
        parser.print_help()
        raise CommandlineError("You must specify a directory to watch")

    return args

def _create_redis_connection(host=None, port=None, db=None):
    log = logging.getLogger("create_redis_connection")

    if host is None:
        host = _redis_host
    if port is None:
        port = _redis_port
    if db is None:
        db = _redis_db

    log.info("connecting to {0}:{1} db={2}".format(host, port, db))
    return redis.StrictRedis(host=host, port=port, db=db)

def _process_message(args, redis, message):
    log = logging.getLogger("_process_message")
    message_text = message["data"].decode("utf-8")
    redis_key, expected_count_str = message_text.split()
    expected_count = int(expected_count_str)
    members = redis.smembers(redis_key)
    if len(members) == expected_count:
        log.info("received key {0} with {1} set members".format(redis_key, 
                                                                len(members)))
    else:
        log.error("received key {0} with {1} set members expected {2}".format(
                  redis_key, len(members), expected_count))

    # we don't need this key anymore
    redis.delete(redis_key) 

    for member in members:
        file_name = member.decode("utf-8")
        path = os.path.join(args.watch_path, file_name)
        log.info("removing {0}".format(path))
        os.unlink(path)

def main():
    """
    main entry point for the program
    The return value will be the returncode for the program
    """
    _initialize_stderr_logging()
    log = logging.getLogger("main")
    log.info("program starts")

    args = _parse_commandline()

    redis = _create_redis_connection()
    subscriber = redis.pubsub()
    subscriber.subscribe(args.notification_channel)
    for message in subscriber.listen():
        if message["type"] == "subscribe": 
            log.info("subscribed to {0}".format(message["channel"]))
        elif message["type"] == "message":
            _process_message(args, redis, message)
        else:
            log.error("Unknown message {0}".format(message))

    log.info("program completes return code = 0")
    return 0

if __name__ == "__main__":
    sys.exit(main())
    