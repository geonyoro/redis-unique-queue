redis-unique-queue
====================
A module that combines the use of redis in-built data types to build a unique queue for processing and expiry.

Usage
=====
Items are put on the queue with an optional key. A duplicate key cannot be added until either: 
- the previous key that was added has been fully processed.
- expiry_in_seconds have passed.

Examples
--------
.. code-block:: python

    import time

    import redis

    import ruqueue

    expiry_in_seconds = 1
    conn = redis.Redis()
    uqueue = ruqueue.Queue(redis_conn=conn, expiry_in_seconds=expiry_in_seconds)
    uqueue.put(100)
    print("Expected size 1:", uqueue.qsize())

    uqueue.put(100)  # not added again
    print("Expected size 1:", uqueue.qsize())

    uqueue.put(200)  # not added again
    print("Expected size 2:", uqueue.qsize())

    # get an item
    print("Expected item 100:", uqueue.get())

    # go past expiry seconds to test
    time.sleep(expiry_in_seconds)

    # item 100 has expired
    print(
        "Items that expired:", uqueue.clear_expired()
    )  # [100] -> a list of items that expires

    print("Expected size 1:", uqueue.qsize())
    print("Expected item 200:", uqueue.get())
    # we have finished handling 200
    uqueue.task_done(200)
    print("Expected size 0:", uqueue.qsize())

    print("Expected item None:", uqueue.get())
