import logging
import pickle
import random
import string
import time
from typing import Callable

import redis

LOGGER = logging.getLogger("redis_unique_queue")


def generate_random_string(length):
    return "".join(
        [random.choice(string.ascii_letters + string.digits) for _ in range(length)]
    )


def default_key_gen_func(item):
    return str(item)


class Queue:
    """
    Redis extended producer queue that ensures every item pushed onto it is unique.

    The interface is modelled to have `some` of the same methods as queue.Queue.
    When an item is fetched from the queue, a bound is set on how long processing will
    take.
    """

    def __init__(
        self,
        redis_conn: redis.client.Redis,
        expiry_in_seconds: int,
        var_prefix: str = "",
        key_gen_func: Callable = default_key_gen_func,
    ):
        """
        @param var_prefix
        @param key_gen_func is not provided, it will be auto-generated.

        """
        self._redis_conn = redis_conn

        assert isinstance(expiry_in_seconds, int)
        assert expiry_in_seconds > 0
        self._expiry_in_secs = expiry_in_seconds

        self._key_gen_func = key_gen_func
        self._setup_vars(var_prefix)

    def put(self, item) -> bool:
        """
        push an item to the internal list, if it doesn't exist in the set.

        if @param key is not provided, the str(item) is used as the key.
        """
        key = self._key_gen_func(item)
        LOGGER.debug("Calculated key %s from item %s.", key, item)

        self.clear_expired()

        if self._sismember(key):
            LOGGER.debug("Key %s already existed in the set.", key)
            return False

        pickled_item = pickle.dumps({"key": key, "item": item})
        pipeline = self._pipeline()
        self._sadd(key, conn=pipeline)
        self._rpush(pickled_item, conn=pipeline)
        pipeline.execute()

        return True

    def task_done(self, item):
        key = self._key_gen_func(item)
        return self._remove_item(key)

    def clear_expired(self):
        removed = []
        for key in self._zrangebyscore(0, int(time.time())):
            removed.append(key)
            self._remove_item(key)
            LOGGER.debug("Removed expired key %s.", key)
        return removed

    def qsize(self):
        return self._scard()

    def get(self):
        pickled_item = self._lpop()
        if not pickled_item:
            return None
        unpickled_item = pickle.loads(pickled_item)

        key = unpickled_item["key"]
        item = unpickled_item["item"]
        expiry_time = int(time.time() + self._expiry_in_secs)
        self._zadd({key: expiry_time})
        return item

    def get_key_for_item(self, item):
        return self._key_gen_func(item)

    def _setup_vars(self, var_prefix):
        if not var_prefix:
            var_prefix = generate_random_string(6)

        self._vars = {
            "set": f"{var_prefix}:set",
            "list": f"{var_prefix}:list",
            "zset": f"{var_prefix}:expiry:zset",
        }
        LOGGER.debug("Vars created are %s", self._vars)

    def _sismember(self, key, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.sismember(self._vars["set"], key)

    def _scard(self):
        return self._redis_conn.scard(self._vars["set"])

    def _sadd(self, key, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.sadd(self._vars["set"], key)

    def _srem(self, key, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.srem(self._vars["set"], key)

    def _zrangebyscore(self, *args, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.zrangebyscore(self._vars["zset"], *args)

    def _zrem(self, key, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.zrem(self._vars["zset"], key)

    def _zadd(self, mapping, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.zadd(self._vars["zset"], mapping)

    def _lpop(self, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.lpop(self._vars["list"])

    def _rpush(self, item, conn=None):
        if conn is None:
            conn = self._redis_conn
        return conn.rpush(self._vars["list"], item)

    def _pipeline(self):
        return self._redis_conn.pipeline()

    def _remove_item(self, key):
        self._srem(key)
        self._zrem(key)
