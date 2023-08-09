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


class RedisUniqueQueue:
    """
    Redis extended producer queue that ensures every item pushed onto it is unique and expires if processing fails.
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

    def add(self, item, key: str) -> bool:
        """
        push an item to the internal list, if it doesn't exist in the set.
        """
        self.clear_expired()
        set_name = self._vars["set"]
        if self._redis_conn.sismember(set_name, key):
            LOGGER.debug("Key %s already existed in the set %s.", key, set_name)
            return False

        # TODO: wrap this as a transaction
        self._redis_conn.sadd(set_name, key)
        pickled_item = pickle.dumps({"key": key, "item": item})
        self._redis_conn.rpush(self._vars["list"], pickled_item)

        return True

    def clear_expired(self):
        removed = []
        for key in self._redis_conn.zrangebyscore(
            self._vars["zset"], 0, int(time.time())
        ):
            # remove from the set
            self._redis_conn.srem(self._vars["set"], key)
            # remove from the zset
            self._redis_conn.zrem(self._vars["zset"], key)
            removed.append(key)
        return removed

    def get(self):
        pickled_item = self._redis_conn.lpop(self._vars["list"])
        unpickled_item = pickle.loads(pickled_item)

        key = unpickled_item["key"]
        item = unpickled_item["item"]
        expiry_time = int(time.time() + self._expiry_in_secs)
        self._redis_conn.zadd(
            self._vars["zset"],
            {key: expiry_time},
        )
        return item
