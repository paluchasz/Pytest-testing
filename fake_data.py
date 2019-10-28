import json
import pandas as pd


# Let's say we want to test a sum function
def create_redis_data():
    # Faking redis data
    test_1 = {'digits': [2, 4, 1, 5], 'sum': 12}
    test_2 = {'digits': [3, -2, 6, -8], 'sum': -1}
    test_3 = {'digits': [20, 0, -30, 40], 'sum': 30}
    tests = [test_1, test_2, test_3]
    for i, test in enumerate(tests):
        with open("/home/paluchasz/Dropbox/programming/pytest_examples/data/redis_data" + str(i) + ".json",
                  "w") as file:
            json.dump(test, file)


def create_postgres_data():
    test_1 = [{'name': 'Alice', 'age': 20, 'id': 0}, {'name': 'Bob', 'age': 30, 'id': 1},
              {'name': 'Charles', 'age': 25, 'id': 2}]
    test_2 = [{'name': 'Alice', 'age': 50, 'id': 0}, {'name': 'Bob', 'age': 45, 'id': 1},
              {'name': 'Charles', 'age': 60, 'id': 2}]
    test_3 = [{'name': 'Alice', 'age': 18, 'id': 0}, {'name': 'Bob', 'age': 16, 'id': 1},
              {'name': 'Charles', 'age': 15, 'id': 2}]
    tests = [test_1, test_2, test_3]

    for i, test in enumerate(tests):
        with open("/home/paluchasz/Dropbox/programming/pytest_examples/data/postgres_table" + str(i) + ".csv",
                  "w") as file:
            pd.DataFrame(test).to_csv(file)

    age_sums = [sum(row['age'] for row in test) for test in tests]
    with open("/home/paluchasz/Dropbox/programming/pytest_examples/data/postgres_test_ground_truth.json",
              "w") as file:
        json.dump(age_sums, file)
    pass


if __name__ == '__main__':
    # create_redis_data()
    create_postgres_data()

