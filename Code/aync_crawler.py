import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial

import requests
import json

queue = asyncio.Queue()
path_root = "/home/user/workspace/Scraper/"


"""
Producer, simplely takes the urls and dump them into the queue
"""


async def produce(queue):
    with open(path_root + 'input/sniff.txt', 'r', encoding='utf-8') as inputf:
        for uid in inputf:
            await queue.put(uid)
        await queue.put(None)  # poison pill to signal all the work is done


"""
Helper function to send request and manipulate response
"""


async def async_request(uid, loop, callback=None):
    print("Sending request to: " + uid)
    """
    This is a canonical way to turn a synchronized
    routine to async. event_loop.run_in_executor,
    by default, takes a new thread from ThreadPool.
    It is also possible to change the executor to ProcessPool.
    """
    access_token = ""
    root = "https://graph.facebook.com/v3.3/"
    params = "?fields=email,address,birthday,first_name,last_name,middle_name&access_token="
    url = root + uid + params + access_token

    ret = await loop.run_in_executor(ThreadPoolExecutor(),
                                     partial(requests.get, timeout=5), url)
    obj = json.loads(ret)
    if callback is not None:
        callback(uid, obj)


"""
Consumer with an infinite loop. It only stops if there is a poison pill.
"""


async def consume(queue, loop):
    with open(path_root + 'output/graph_api.txt', 'a+', encoding='utf-8') as f:
        def write_to_file(uid, obj):
            email = obj["email"] if 'email' in obj else 'null'
            address = obj["address"] if 'address' in obj else 'null'
            birthday = obj["birthday"] if 'birthday' in obj else 'null'
            first_name = obj["first_name"] if 'first_name' in obj else ''
            last_name = obj["last_name"] if 'last_name' in obj else ''
            middle_name = obj["middle_name"] if 'middle_name' in obj else ''
            full_name = first_name + " " + middle_name + " " + last_name
            f.write(uid + "," + email + "," + address + "," + birthday + "," +
                    full_name + "\n")
        while True:
            # coroutine will be blocked if queue is empty
            item = await queue.get()
            if item is None:  # if poison pill is detected, exit the loop
                break
            await async_request(item, loop, write_to_file)
            # signal that the current task from the queue is done
            # and decrease the queue counter by one
            queue.task_done()

"""
Driver
"""
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue(loop=loop)
    producer_coro = produce(queue)
    consumer_coro = consume(queue, loop)
    loop.run_until_complete(asyncio.gather(producer_coro, consumer_coro))
    loop.close()
