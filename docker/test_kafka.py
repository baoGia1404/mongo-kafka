import names
import pymongo
from bloom_filter import BloomFilter
import random
from datetime import datetime
import logging

logging.info('Begin to populate data.')
bloom = BloomFilter(max_elements=10000, error_rate=0.001)
set_name = set()
dict_gender = dict()
client = pymongo.MongoClient("mongodb://mongo1:27017,mongo2:27017,mongo3:27017")
db = client["kafka_sample"]
coll = db["sample"]
range_name = 5000
logging.info('Start generating name.')
while len(set_name) <= range_name:
    female = names.get_first_name(gender='female')
    male = names.get_first_name(gender='male')
    set_name.add(male)
    set_name.add(female)
    dict_gender[male] = 'male'
    dict_gender[female] = 'female'
    logging.info('{} - {}'.format(male, female))
list_name = list(set_name)
logging.info('Finish generating name.')
logging.info('Start inserting data.')
count = 0
while count <= 2000000:
    index = random.randrange(range_name - 1, 0, -1)
    amount = random.randrange(10000, 10, -1)
    name = list_name[index]
    gender = dict_gender[name]
    doc = {
        'name': name,
        'gender': gender,
        'amount': amount,
        'update_count': 1
    }
    now = datetime.now()
    if name not in bloom:
        doc['created_time'] = now
        doc['updated_time'] = now
        _id = coll.insert_one(doc).inserted_id
        _type = 'insert'
        bloom.add(name)
    else:
        _id = coll.update_one(
            {
                'name': name,
                'gender': gender
             },
            {
                '$inc': {
                  'update_count': 1
                },
                '$set': {
                    'updated_time': now,
                    'amount': amount
                }
            }).upserted_id
        _type = 'update'
    logging.info('{} - {} - {}'.format(count, _id, _type))
    count = count + 1

client.close()

