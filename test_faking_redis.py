import json
from pathlib import Path

import pytest
from birdisle import redis

TEST_ROOT = Path(__file__).resolve().parent
rc = None
redis_key_format = "key:{i}"


def load_data_and_connect():
    global rc
    if not rc:
        rc = redis.StrictRedis(decode_responses=True)
    path = TEST_ROOT / "data"
    files = Path.glob(path, "redis*.json")

    tests = []
    for file in files:
        with open(str(file), "r") as infile:
            data = json.load(infile)
        tests.append(data)

    return [(test_case['digits'], test_case['sum']) for test_case in tests]


def create_redis_data(rc, key_format, values):
    for i, value in enumerate(values):
        redis_key = key_format.format(i=i)
        rc.set(redis_key, value)


def delete_redis_keys(rc, key_format):
    for redis_key in rc.scan_iter(key_format.format(i='*')):
        rc.delete(redis_key)


def sum_redis_key_values(rc, key_format):
    total = 0
    for redis_key in rc.scan_iter(key_format.format(i="*")):
        total += int(rc.get(redis_key))
    return total


@pytest.mark.parametrize('values, result', load_data_and_connect())
def test_sum_keys(values, result):
    global rc, redis_key_format
    create_redis_data(rc, redis_key_format, values)
    redis_keys_sum = sum_redis_key_values(rc, redis_key_format)
    delete_redis_keys(rc, redis_key_format)
    assert redis_keys_sum == result


if __name__ == '__main__':
    tests = load_data_and_connect()
    for test in tests:
        test_sum_keys(test[0], test[1])
    pass