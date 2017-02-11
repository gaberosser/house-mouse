__author__ = 'gabriel'
from pymongo import MongoClient
import collections
import datetime

if __name__ == '__main__':
    client = MongoClient()
    db = client.gabs
    coll = db.zoopla_for_sale

    agents = collections.defaultdict(list)

    fields_to_keep = (
        'price', 'first_published_date', 'num_bedrooms', 'num_bathrooms', 'num_recepts',
        'post_town', 'postcode_area', 'property_type',
    )
    for t in coll.find():
        a = t['agent_name']
        dat = dict([(k, t[k]) for k in fields_to_keep])
        agents[a].append(dat)

    agent_count = dict([(a, len(agents[a])) for a in agents])
