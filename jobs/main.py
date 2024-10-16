from kafka import KafkaProducer
import json
import time
import requests
import random


def get_driver(year=None):
    """
    Parameters
    ----------
    year: int
        An optional parameter that specifies the year to be queried.
    Returns
    -------
    dict

    Columns:
        driverId: str
        permanentNumber: int
        code: str
        url: str
        givenName: str
        familyName: str
        dateOfBirth: str
        nationality: str
    """
    if year:
        url = f'https://ergast.com/api/f1/{year}/drivers.json'
    else:
        url = 'https://ergast.com/api/f1/drivers.json'
    r = requests.get(url)

    assert r.status_code == 200, 'Cannot connect to Ergast API'
    drivers = r.json()["MRData"]["DriverTable"]["Drivers"]

    driver = random.choice(drivers)
    result = {
        "driverId": driver["driverId"],
        "permanentNumber": int(driver["permanentNumber"]),
        "code": driver["code"],
        "url": driver["url"],
        "givenName": driver["givenName"],
        "familyName": driver["familyName"],
        "dateOfBirth": driver["dateOfBirth"],
        "nationality": driver["nationality"]
    }

    return result


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

if __name__ == "__main__":
    while True:
        registered_user = get_driver(2022)
        print(registered_user)
        producer.send("kafka_stream", registered_user)
        time.sleep(3)
