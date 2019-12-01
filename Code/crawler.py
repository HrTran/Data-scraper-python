import os
import platform
import sys
import traceback
import mysql.connector
import asyncio
import datetime
import multiprocessing as mp
from random import randint
from shutil import which
import logging
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


sys.path.insert(0, '/usr/lib/chromium-browser/chromedriver')
FIREFOXPATH = which("firefox")
CHROMEPATH = which("chrome") or which("chromium")
queue = asyncio.Queue()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger_dir = "log/"
start = datetime.time(23, 30, 0)
end = datetime.time(23, 59, 0)


def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def init_webdriver():
    """Simple Function to initialize and configure Webdriver"""
    if FIREFOXPATH is not None:
        print(FIREFOXPATH)
        from selenium.webdriver.firefox.options import Options

        options = Options()
        options.binary = FIREFOXPATH
        options.add_argument("--headless")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-infobars")
        options.add_argument("--mute-audio")
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-dev-shm-usage")
        return webdriver.Firefox(options=options,
                                 executable_path="../bin/geckodriver",
                                 log_path="geckodriver.log")

    elif CHROMEPATH is not None:
        print(CHROMEPATH)
        from selenium.webdriver.chrome.options import Options

        options = Options()
        options.binary_location = CHROMEPATH
        options.add_argument("--headless")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-infobars")
        options.add_argument("--mute-audio")
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-dev-shm-usage")
        return webdriver.Chrome(options=options,
                                executable_path="../bin/chromedriver",
                                service_args=['--verbose'],
                                service_log_path="chromedriver.log")


def thr(index):
    os.chdir("/home/user/workspace/Scraper/Code/")
    code = "52367427"
    arbitrary = randint(0, 9)
    pos = randint(2, 8)
    code.replace(code[pos], arbitrary)
    link = ""
    return link


def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end


def generate_name(queue):
    firstname_file = "input/.txt"
    lastname_file = "input/.txt"
    fnames = [line.replace("\n", "") for line in open(firstname_file, newline='\n')]
    lnames = [line.replace("\n", "") for line in open(lastname_file, newline='\n')]
    logger = setup_logger("producer", logger_dir + 'producer.log')
    logger.info("fnames's size: " + str(len(fnames)))
    logger.info("lnames's size: " + str(len(lnames)))
    for fname in fnames:
        for lname in lnames:
            logger.info("Put " + fname + " " + lname)
            queue.put(fname + " " + lname)


def consumer(queue, index):
    logger_name = "consumer_" + index
    logger = setup_logger(logger_name, logger_dir + logger_name + '.log')
    while True:
        # coroutine will be blocked if queue is empty
        now = datetime.datetime.now().time()
        while time_in_range(start, end, now):
            time.sleep(x * y)
        logger.info("queue's size: " + str(queue.qsize()))
        item = queue.get()
        if item is None:
            break
        fname = item.split(" ")[0]
        lname = item.split(" ")[1]
        logger.info("item = " + item + ", fname = " + fname +
                    " ,lname = " + lname)
        get_info(fname, lname, index)
        logger.info("done")
        # signal that the current task from the queue is done
        # and decrease the queue counter by one
        queue.task_done()


def get_info(fname, lname, index):
    """ logger into our own profile """
    try:
        # global driver
        driver = None
        options = Options()
        full_name = fname + "_" + lname
        log_file = logger_dir + "consumer_log_" + index
        logger = setup_logger(full_name, full_name + '.log')
        loggerE = setup_logger(full_name, full_name + '_err.log',
                               logging.ERROR)

        #  Code to disable notifications pop up of Chrome Browser
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-infobars")
        options.add_argument("--mute-audio")
        options.add_argument("--headless")
        # options.add_argument('--proxy-server=https://{}'.format(proxy))
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-dev-shm-usage")

        try:
            platform_ = platform.system().lower()
            if platform_ in ['linux', 'darwin']:
                driver = init_webdriver()
            else:
                driver = init_webdriver()
        except Exception:
            print(full_name + ": Kindly replace the Chrome Web Driver with the"
                  " latest one from http://chromedriver.chromium.org/downloads"
                  "\nYour OS: {}".format(platform_))
            traceback.print_exc()
            # exit()

        driver.get("https://")
        delay = 30  # seconds
        # driver.maximize_window()

        # filling the form

        # clicking on login button
        
        isLoaded = False
        nTry = 0
        while isLoaded is not True and nTry < 3:
            try:
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.ID, 'reg')))
                logger.info(full_name + ": Page is ready!")
                isLoaded = True
            except Exception:
                loggerE.error(full_name + ": Loading took too much time!")
                nTry += 1
                time.sleep(60)

        list_person = []
        getData1(driver, list_person, logger, loggerE)
        getData2(driver, list_person, logger, loggerE)
        getData3(driver, list_person, logger, loggerE)
        logger.info(full_name + " - List size: " + str(len(list_person)))
        bulkInsertIntoPeopleTable(list_person, logger, loggerE)
        closeLogging(logger)
        closeLogging(loggerE)
        driver.close()
    except Exception:
        loggerE.error(full_name + ": There's some error in get_info.")
        loggerE.error(sys.exc_info())
        closeLogging(logger)
        closeLogging(loggerE)
        if driver is not None:
            driver.close()


def closeLogging(logger):
    for _ in list(logger.handlers):
        logger.removeHandler(_)
        _.flush()
        _.close()


def getData1(driver, list_person, logger, loggerE):
    logger.info("\n======================Data1=======================")
    # try catch and add logic Xpath here


def getData2(driver, list_person, logger, loggerE):
    logger.info("\n=====================Data2=======================")
    # try catch and add logic Xpath here


def getData3(driver, list_person, logger, loggerE):
    logger.info("\n=======================Data3=========================")
    # try catch and add logic Xpath here


def bulkInsertIntoPeopleTable(list_person, logger, loggerE):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='database',
                                             user='user',
                                             password='password')
        cursor = connection.cursor()
        add_person = ("INSERT INTO fbdata.PERSON_TMP "
                      "(full_name, first_name, last_name, age, gender, "
                      " date_of_birth, email, phone, address, other_emails,"
                      " other_phones) "
                      " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        cursor.executemany(add_person, list_person)
        connection.commit()
        logger.info("Record inserted successfully into python_users table")
    except mysql.connector.Error as error:
        connection.rollback()  # rollback if any exception occured
        loggerE.error("Failed inserting record into python_users table {}"
                      .format(error))
    finally:
        # closing database connection.
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MySQL connection is closed")


def main():
    with mp.Pool(2) as pool:
        data_q = mp.JoinableQueue()
        p1 = pool.Process(target=generate_name, args=(data_q, ))
        p2 = pool.Process(target=consumer, args=(data_q, "1"))
        p1.start()
        p2.start()
        p1.join()
        p2.join()


if __name__ == '__main__':
    # get things rolling
    main()

