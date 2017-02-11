__author__ = 'gabriel'
from pymongo import MongoClient
import collections
import json
import datetime
import time
import requests
from settings import ZOOPLA_TOKEN, ZOOPLA_DELAY
import consts
from log import get_console_logger
logger = get_console_logger(__name__)
PROPERTY_LISTING_URL = "http://api.zoopla.co.uk/api/v1/property_listings.json"
PAGE_SIZE = 100  # maximum permissible


LISTING_PARSER = {
    'first_published_date': lambda t: datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S'),
    'last_published_date': lambda t: datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S'),
    'date': lambda t: datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S'),
    'num_bathrooms': int,
    'num_bedrooms': int,
    'num_floors': int,
    'num_recepts': int,
    'price': int,
    'percent': lambda t: float(t.replace('%', '')),
}


class ZooplaApi(object):
    limit_per_second = 100
    limit_per_hour = 100

    def __init__(self, api_key, calls_made_this_hour=0):
        self.api_key = api_key
        self.second_time = datetime.datetime.now()
        self.hour_time = datetime.datetime.now()
        self.total_calls = 0
        self.calls_this_second = 0
        self.calls_this_hour = calls_made_this_hour

    def increment_call_counts(self):
        now = datetime.datetime.now()
        self.total_calls += 1

        if (now - self.second_time).total_seconds() > 1:
            self.calls_this_second = 1
            self.second_time = now
        else:
            self.calls_this_second += 1

        if (now - self.hour_time).total_seconds() / 3600. > 1:
            self.calls_this_hour = 1
            self.hour_time = now
        else:
            self.calls_this_hour += 1

    def check_limits(self):
        """
        Test whether we have exceeded any limits and, if so, wait
        :return:
        """
        if self.calls_this_second == self.limit_per_second:
            # wait until the second is up
            wait_til = self.second_time + datetime.timedelta(seconds=1)
            wait_for = (wait_til - datetime.datetime.now()).total_seconds()
            logger.info("Exceeded per second limit. Sleeping for %.2f seconds...", wait_for)
            time.sleep(wait_for)

        if self.calls_this_hour == self.limit_per_hour:
            # wait until the hour is up
            wait_til = self.hour_time + datetime.timedelta(hours=1)
            wait_for = (wait_til - datetime.datetime.now()).total_seconds()
            logger.info("Exceeded per second limit. Sleeping for %d minutes...", int(wait_for / 60.))
            time.sleep(wait_for)

    def property_listing(self, **params):
        pages_failed = []
        self.check_limits()
        req = requests.get(PROPERTY_LISTING_URL, params=params)
        self.increment_call_counts()
        if req.status_code != 200:
            raise requests.RequestException(req.content)
        con = json.loads(req.content)
        n_list = con['result_count']
        calls_req = (n_list // PAGE_SIZE + 1) if (n_list % PAGE_SIZE) else n_list // PAGE_SIZE
        logger.info(
            "Found %d results. Will use %d API calls in total.",
            n_list,
            calls_req
        )
        listings = con['listing']
        for i in range(2, calls_req + 1):
            params['page_number'] = i
            self.check_limits()
            req = requests.get(PROPERTY_LISTING_URL, params=params)
            self.increment_call_counts()
            if req.status_code != 200:
                logger.error("Failed to make call %d: %s", i, req.content)
                pages_failed.append(i)
            else:
                con = json.loads(req.content)
                listings.extend(con['listing'])

        # parse them
        for l in listings:
            for k, v in l.items():
                if k in LISTING_PARSER:
                    try:
                        l[k] = LISTING_PARSER[k](v)
                    except Exception:
                        logger.exception("Parsing failed")
            # price changes
            if 'price_change' in l:
                for p in l['price_change']:
                    for k, v in p.items():
                        if k in LISTING_PARSER:
                            try:
                                p[k] = LISTING_PARSER[k](v)
                            except Exception:
                                logger.exception("Price change parsing failed")
        return listings, pages_failed


def make_get_call(url, params):
    req = requests.get(url, params=params)
    if req.status_code != 200:
        raise requests.RequestException(req.content)
    return json.loads(req.content)


if __name__ == "__main__":
    z = ZooplaApi(api_key=ZOOPLA_TOKEN, calls_made_this_hour=0)
    client = MongoClient()
    db = client.gabs
    coll = db.zoopla_for_sale
    failed_pages = collections.defaultdict(list)
    for reg in consts.INNER_LONDON + (consts.CR,):
        for pc in reg:
            # check whether we have already synced this one
            existing_count = coll.find({'postcode_area': pc}).count()
            if existing_count:
                logger.info("Already have %d records for postcode %s. Skipping.", existing_count, pc)
                continue
            # form request data
            params = {
                'api_key': ZOOPLA_TOKEN,
                'postcode': pc,
                'listing_status': 'sale',
                'include_sold': 1,
                'page_size': PAGE_SIZE,
            }
            try:
                listings, pages_failed = z.property_listing(**params)
                if len(listings) == 0:
                    logger.warn("No listings retrieved for postcode %s.", pc)
                    continue
            except Exception:
                logger.exception("Failed to make first call for postcode %s.", pc)
            else:
                failed_pages[pc].extend(pages_failed)
                for l in listings:
                    l['postcode_area'] = pc

                coll.insert_many(listings, ordered=False)
