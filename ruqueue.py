import logging
import pickle
import random
import string
import time

import redis

LOGGER = logging.getLogger("redis_unique_queue")


def generate_random_string(length):
    return "".join(
        [random.choice(string.ascii_letters + string.digits) for _ in range(length)]
    )


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
    ):
        """
        If var_prefix is not provided, it will be auto-generated.
        """
        self._redis_conn = redis_conn

        assert isinstance(expiry_in_seconds, int)
        assert expiry_in_seconds > 0
        self._expiry_in_secs = expiry_in_seconds

        if not var_prefix:
            var_prefix = generate_random_string(6)

        self._vars = {
            "set": f"{var_prefix}:set",
            "list": f"{var_prefix}:list",
            "zset": f"{var_prefix}:expiry:zset",
        }
        LOGGER.debug("Vars created are %s", self._vars)

    def _sismember(self, key):
        return self._redis_conn.sismember(self._vars["set"], key)

    def _sadd(self, key):
        return self._redis_conn.sadd(self._vars["set"], key)

    def _srem(self, key):
        return self._redis_conn.srem(self._vars["set"], key)

    def _zrangebyscore(self, *args):
        return self._redis_conn.zrangebyscore(self._vars["zset"], *args)

    def _zrem(self, key):
        return self._redis_conn.zrem(self._vars["zset"], key)

    def _zadd(self, mapping):
        return self._redis_conn.zadd(self._vars["zset"], mapping)

    def _lpop(self):
        return self._redis_conn.lpop(self._vars["list"])

    def put(self, item, key: str) -> bool:
        """
        push an item to the internal list, if it doesn't exist in the set.
        """
        self.clear_expired()

        if self._sismember(key):
            LOGGER.debug("Key %s already existed in the set.", key)
            return False

        # TODO: wrap this as a transaction
        self._sadd(key)
        pickled_item = pickle.dumps({"key": key, "item": item})
        self._redis_conn.rpush(self._vars["list"], pickled_item)

        return True

    def clear_expired(self):
        removed = []
        for key in self._zrangebyscore(0, int(time.time())):
            self._srem(key)
            self._zrem(key)
            removed.append(key)
        return removed

    def get(self):
        pickled_item = self._lpop()
        unpickled_item = pickle.loads(pickled_item)

        key = unpickled_item["key"]
        item = unpickled_item["item"]
        expiry_time = int(time.time() + self._expiry_in_secs)
        self._zadd({key: expiry_time})
        return item