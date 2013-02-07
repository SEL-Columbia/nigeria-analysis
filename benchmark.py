import time
import json
import sys

from pybamboo.dataset import Dataset
from pybamboo.connection import Connection
from pybamboo.exceptions import PyBambooException

def print_time(func):
    """
    @print_time

    Put this decorator around a function to see how many seconds each
    call of this function takes to run.
    """
    def wrapped_func(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        seconds = end - start
        outfile.write("SECONDS: %s\n" % seconds)
        return result
    return wrapped_func



URL = 'http://localhost:8080'
conn = Connection(url=URL)
DIR = 'csvs/'
# choosing between 100, 1000, 10000, 100000
TESTING = 100
outfile = open('%s.log' % TESTING, 'wb')

# test on upload
test_str = str(TESTING)
water_file = DIR + test_str + '/water.csv'
education_file = DIR + test_str + '/education.csv'

@print_time
def create_dataset(connection, path):
    dataset = Dataset(connection=connection, path=path)
    state = dataset.get_info()['state']
    outfile.write('dataset created: %s\n' % dataset.id)
    outfile.write('initial state: %s\n' % state)
    while state == 'pending':
        time.sleep(1)
        state = dataset.get_info()['state']
    outfile.write('final state: %s\n' % state)
    outfile.write('finished importing dataset: %s\n' % dataset.id)
    return dataset

@print_time
def add_calculation(connection, dataset, name, formula):
    pass

try:
    water_dataset = create_dataset(connection=conn, path=water_file)
    water_dataset.delete()
except PyBambooException as e:
    outfile.write('pybamboo error: %s\n' % e)
try:
    edu_dataset= create_dataset(connection=conn, path=education_file)
    edu_dataset.delete()
except PyBambooException as e:
    outfile.write('pybamboo error: %s\n' % e)
outfile.close()
