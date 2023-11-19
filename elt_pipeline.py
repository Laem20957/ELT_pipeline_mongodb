import json
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

url = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
collection = "test_collection"
port = 27017
database = "TestDB"
username = "admin_cluster"
password = "admin"


def get_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")
    dataset = soup.find(xpath="//pre[@style='word-wrap: break-word; white-space: pre-wrap;']")
    dataset = soup.text.strip()
    json_data = json.loads(dataset)
    return json_data


def get_engine(port, database, username, password):
    client = MongoClient(f"mongodb://{quote(username)}:{quote(password)}@localhost:{port}/?authMechanism=SCRAM-SHA-1&authSource={quote(database)}")
    return client


def write_records(client, coll, rows):
    db = client[database]
    coll = db[collection]
    coll.delete_many({})
    coll.insert_many(rows)
    return coll


def main(port, database, collection, username, password):
    client = get_engine(port, database, username, password)
    json_data = get_data(url)
    write_records(client, collection, json_data)


main(port, database, collection, username, password)
