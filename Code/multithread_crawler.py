import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from datetime import datetime
from time import sleep
import multiprocessing as mp

import requests
import importlib

queue = asyncio.Queue()
scraper = importlib.import_module("scraper")


"""
Producer, simplely takes the urls and dump them into the queue
"""


def producer(queue):
    with open('urls.txt', 'r', encoding='utf-8') as inputf:
        for url in inputf:
            queue.put(url)
        queue.put(None)  # poison pill to signal all the work is done


"""
Helper function to send request and manipulate response
"""


def async_request(url, callback=None):
    print()
    print("[" + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + "] Crawling: " +
          url + ", queue's size: " + str(queue.qsize()))

    """
    This is a canonical way to turn a synchronized routine to
    async.event_loop.run_in_executor,
    by default, takes a new thread from ThreadPool.
    It is also possible to change the executor to ProcessPool.
    """
    # ret = await loop.run_in_executor(ThreadPoolExecutor(),
    #                                  partial(requests.get, timeout=5), url)
    printSomething(url)
    if callback is not None:
        # insert to DB
        callback(url)


def printSomething(url):
    print("url=" + url)


"""
Consumer with an infinite loop. It only stops if there is a poison pill.
"""


def consumer(queue):
    with open('output.txt', 'w', encoding='utf-8') as f:
        def write_to_file(url):
            f.write(url + "\n")
        while True:
            # coroutine will be blocked if queue is empty
            item = queue.get()
            if item is None:
            async_request(item, write_to_file)
            # signal that the current task from the queue is done
            # and decrease the queue counter by one
            queue.task_done()


"""
Driver
"""
if __name__ == '__main__':
    with mp.Pool(8) as pool:
        p1 = pool.Process(target=scraper.thr, args=(1, ))
        p2 = pool.Process(target=scraper.thr, args=(2, ))
        p3 = pool.Process(target=scraper.thr, args=(3, ))
        p4 = pool.Process(target=scraper.thr, args=(4, ))
        p5 = pool.Process(target=scraper.thr, args=(5, ))
        p6 = pool.Process(target=scraper.thr, args=(6, ))
        p7 = pool.Process(target=scraper.thr, args=(7, ))
        p8 = pool.Process(target=scraper.thr, args=(8, ))
        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()
        p8.start()
        p1.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()
        p8.join()
