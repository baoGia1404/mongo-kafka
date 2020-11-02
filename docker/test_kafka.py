import names
import pymongo
from bloom_filter import BloomFilter
import random
from datetime import datetime
import logging


logger = logging.getLogger('dev')
logger.setLevel(logging.INFO)
print('Begin to populate data.')
bloom_a = BloomFilter(max_elements=10000, error_rate=0.001)
bloom_b = BloomFilter(max_elements=10000, error_rate=0.001)

set_name = set()
set_company = set()
dict_gender = dict()
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["competition_monday_night"]
rank_a_coll = db["user_rank_a"]
rank_b_coll = db['user_rank_b']
range_name = 5000
print('Start generating name.')
while len(set_name) <= range_name:
    female = names.get_first_name(gender='female')
    male = names.get_first_name(gender='male')
    set_name.add(male)
    set_name.add(female)
    dict_gender[male] = 'male'
    dict_gender[female] = 'female'
    print('{} - {}'.format(male, female))
list_name = list(set_name)
print('Finish generating name.')
print('Start inserting data.')
count = 0
while count <= 2000000:
    index = random.randrange(range_name - 1, 0, -1)
    amount_a = random.randrange(10000, 10, -1)
    amount_b = random.randrange(10000, 10, -1)
    name = list_name[index]
    gender = dict_gender[name]
    doc_a = {
        'player_id': count + 1,
        'name': name,
        'gender': gender,
        'amount': amount_a,
        'update_count': 1,
        'server': 'a'
    }
    doc_b = {
        'player_id': count + 1,
        'name': name,
        'gender': gender,
        'amount': amount_b,
        'update_count': 1,
        'server': 'b'
    }
    now = datetime.now()

    if name not in bloom_a:
        doc_a['created_time'] = now
        doc_a['updated_time'] = now
        a_id = rank_a_coll.insert_one(doc_a).inserted_id
        bloom_a.add(name)
    else:
        a_id = rank_a_coll.update_one(
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
                    'amount': amount_a
                }
            }).upserted_id

    if name not in bloom_b:
        doc_b['created_time'] = now
        doc_b['updated_time'] = now
        b_id = rank_b_coll.insert_one(doc_b).inserted_id
        bloom_b.add(name)
    else:
        b_id = rank_b_coll.update_one(
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
                    'amount': amount_b
                }
            }).upserted_id
    print('{} - {} - {} - {} - {}'.format(count, a_id, b_id, amount_a, amount_b))
    count = count + 1

client.close()

