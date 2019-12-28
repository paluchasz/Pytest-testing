import json
from pathlib import Path

import pgdb
import pytest
import pytest_check as check
import testing.postgresql
import pandas as pd
from sqlalchemy import create_engine

TEST_ROOT = Path(__file__).resolve().parent
CONNECTION, engine, psql = None, None, None
total_test_cases = 0


def fake_postgres():
    global CONNECTION, engine, psql
    psql = testing.postgresql.Postgresql()
    info = psql.dsn()
    CONNECTION = pgdb.connect(user=info['user'], host=info['host'], database=info['database'], port=info['port'])
    engine = create_engine(psql.url())


def disconnect_fake_postgres():
    global psql, CONNECTION
    psql = psql.stop()
    CONNECTION.close()


def rollback_connection():
    CONNECTION.rollback()


def load_data():
    global total_test_cases
    path = TEST_ROOT / "data"
    files = sorted(Path.glob(path, "postgres*.csv"))

    tests = []
    for file in files:
        tests.append(pd.read_csv(str(file)))
    with open(str(path / 'postgres_test_ground_truth.json'), 'r') as infile:
        ground_truth = json.load(infile)

    total_test_cases = len(tests)
    test_case_ids = list(range(total_test_cases))
    return list(zip(tests, ground_truth, test_case_ids))


def sum_ages():
    records = query_database(operation="""SELECT age FROM students""")
    ages = [r.age for r in records]
    return sum(ages)


def query_database(operation):
    cursor = CONNECTION.cursor()
    records = cursor.execute(operation).fetchall()
    cursor.close()
    return records


def create_table(students_df):
    students_df.to_sql('students', engine, if_exists='replace')


@pytest.mark.parametrize('students_df, ground_truth_sum, test_case_id', load_data())
def test_sum_ages(students_df, ground_truth_sum, test_case_id):
    global total_test_cases
    if test_case_id == 0:
        fake_postgres()
    create_table(students_df)
    ages_sum = sum_ages()
    if test_case_id < total_test_cases - 1:
        rollback_connection()
    else:
        disconnect_fake_postgres()
    assert ground_truth_sum == ages_sum


if __name__ == '__main__':
    data = load_data()
    test_sum_ages(data[0][0], data[0][1], 0)
    pass
