import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial

import requests
import json
import urllib.request
from urllib import request as urlrequest
from urllib3.contrib.socks import SOCKSProxyManager
import socket
import urllib.error
import threading

queue = asyncio.Queue()
path_root = "/home/user/workspace/Scraper/"


"""
Producer, simplely takes the urls and dump them into the queue
"""


async def produce(queue):
    file_path = path_root + 'proxies/socks4-10000ms-country_all-.txt'
    # file_path = path_root + 'proxies/test_proxy.txt'
    with open(file_path, 'r', encoding='utf-8') as inputf:
        for proxy in inputf:
            await queue.put(proxy.rstrip())
        await queue.put(None)  # poison pill to signal all the work is done


"""
Helper function to send request and manipulate response
"""


async def async_request(proxy, loop, callback=None):

    """
    This is a canonical way to turn a synchronized
    routine to async. event_loop.run_in_executor,
    by default, takes a new thread from ThreadPool.
    It is also possible to change the executor to ProcessPool.
    """
    # print("Sending request to: " + proxy)
    ret = loop.run_in_executor(ThreadPoolExecutor(),
                               partial(check_socks_proxy, 'socks4'), proxy)
    try:
        result = await asyncio.wait_for(ret, timeout=10.0, loop=loop)
        print(result)
        if callback is not None:
            callback(proxy, result)
    except Exception as e:
        print("Timeout for " + proxy)


def is_OK(proxy):
    try:
        proxy_handler = urllib.request.ProxyHandler({'http': proxy})
        opener = urllib.request.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib.request.install_opener(opener)
        req = urllib.request.Request('http://www.example.com')
        sock = urllib.request.urlopen(req)
        return True
    except urllib.error.HTTPError as e:
        print('Error code: ', e.code)
        return False
    except Exception as detail:
        print("ERROR:", detail)
        return False
    return False


def check_proxy(proxy):
    # print(requests.get('https://google.com', proxies=proxy))
    try:
        req = urlrequest.Request('https://facebook.com')
        req.set_proxy(proxy, 'http')
        response = urlrequest.urlopen(req)
        print(response.read().decode('utf8'))
        return True
    except Exception as e:
        print('Error code: ', e.__cause__)
        return False


def check_socks_proxy(proxy_type, proxy):
    complete_link = proxy_type + '://' + proxy
    print('Sending request to ' + complete_link)
    try:
        proxy = SOCKSProxyManager(complete_link)
        response = proxy.request('GET', 'http://facebook.com/')
        print(response.status)
        if response.status == 200:
            return True
        else:
            return False
    except Exception as e:
        print('Error code: ', e.__cause__)
        return False


"""
Consumer with an infinite loop. It only stops if there is a poison pill.
"""


async def consume(queue, loop):
        def write_to_file(proxy, ret):
            if ret:
                with open(path_root + 'output/usable_proxies-socks4.txt',
                          'a+', encoding='utf-8') as f:
                    f.write(proxy + "\n")
                print("Write " + proxy + " to file complete!")
        while True:
            # coroutine will be blocked if queue is empty
            item = await queue.get()
            if item is None:  # if poison pill is detected, exit the loop
                break
            await async_request(item, loop, write_to_file)
            # signal that the current task from the queue is done
            # and decrease the queue counter by one
            queue.task_done()
        print("DONE...!")



def thr():
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue(loop=loop)
    producer_coro = produce(queue)
    consumer_coro = consume(queue, loop)
    consumer_coro1 = consume(queue, loop)
    consumer_coro2 = consume(queue, loop)
    consumer_coro3 = consume(queue, loop)
    consumer_coro4 = consume(queue, loop)
    consumer_coro5 = consume(queue, loop)
    consumer_coro6 = consume(queue, loop)
    consumer_coro7 = consume(queue, loop)
    loop.run_until_complete(asyncio.gather(producer_coro, consumer_coro,
                                           consumer_coro1, consumer_coro2,
                                           consumer_coro3, consumer_coro4,
                                           consumer_coro5, consumer_coro6,
                                           consumer_coro7))
    loop.close()


def main():
    num_threads = 10
    threads = [threading.Thread(target=thr,
                                args=(i,)) for i in range(num_threads)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    print("bye")



"""
Driver
"""
if __name__ == '__main__':
    thr()
