import time
import json
import sys
import os

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
curr_time = time.strftime('%Y-%m-%d-%H-%M-%S')
hostname = os.uname()[1]

# test on upload
test_str = str(TESTING)
water_file = DIR + test_str + '/water.csv'
education_file = DIR + test_str + '/education.csv'

def run_test_suite(dataset_file_path_list):
    alldata = []
    for dataset_name in dataset_file_path_list:
        d = {}
        d['hostname'] = os.uname()[1]
        d['bamboo_url'] = URL
        d['git_sha'] = '' # TODO: pull in git-sha/version from pybamboo/bamboo
        d['unix_time'] = time.time()
        conn = Connection(url=URL)
        dataset = Dataset(connection=conn, path=dataset_name)
        d['import_time'] = time_till_import_is_finished(dataset)
        info = dataset.get_info()
        d['row'] = info['num_rows']
        d['col'] = info['num_columns']
        d['add_1_calculations_time'] = time_to_add_1_calculations(dataset)
        d['add_5_calculations_time'] = time_to_add_5_calculations(dataset)
        d['update_1_time'] = time_to_add_1_update(dataset)
        d['update_5_time'] = time_to_add_5_update(dataset)
        dataset.delete()
        alldata.append(d)
    return alldata

def write_to_csv(dict_list, outfile):
    """ Take a dict_list, like [{a: 'foo', b: 'baz', ...}, {a: 'foo2', ...}...]
      into a csv like 
        a,b,...
        foo,baz,...
        foo2,..."""
    keys = dict_list[0].keys()
    keystring = ",".join(keys)
    outfile.write(keystring + "\n")
    for item in dict_list:
        values = [str(item.get(k,'')) for k in keys]
        valstring = ",".join(values)
        outfile.write(valstring + "\n")

# time_function(import_is_finished)

def time_to_add_1_calculations(dataset):
    info = dataset.get_info()
    #print info['schema']
    #for itemkey,item in info['schema']:
    #    if item['simpletype'] == 'float':
    #        calc_col_1 = itemkey
    #        break
    calc_col_1 = '_gps_latitude'
    before = time.time()
    dataset.add_calculation('true = "True"')
    after = time.time()
    return after - before

def time_to_add_5_calculations(dataset):
    info = dataset.get_info()
    calcs = ['true = "T"', 'f = "F"', 'one = "1"', 'two = "2"', 'to = "To"']
    # TODO: more sophisticated calculations?
    #for itemkey,item in info['schema']:
    #    if item['simpletype'] == 'float':
    #        calc_col_1 = itemkey
    #        break
    calc_col_1 = '_gps_latitude'
    before = time.time()
    for calc in calcs:
        dataset.add_calculation(calc)
    after = time.time()
    return after - before

def time_to_add_1_update(dataset):
    # TODO: more sophisticated updates
    update = [{"mylga" : "test"}]
    before = time.time()
    dataset.update_data(update)
    after = time.time()
    return after - before

def time_to_add_5_update(dataset):
    updates = [{"mylga" : "test"},
               {"mylga" : "test1"},
               {"mylga" : "test2"},
               {"mylga" : "test3"},
               {"mylga" : "test4"}]
    before = time.time()
    dataset.update_data(updates)
    after = time.time()
    return after - before

def time_till_import_is_finished(dataset):
    before = time.time()
    state = dataset.get_info()['state']
    while state == 'pending':
        time.sleep(1)
        state = dataset.get_info()['state']
    after = time.time()
    return after - before

@print_time
def add_calculation(connection, dataset, name, formula):
    pass


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

def _run_with(connection, data_file_list):
    for data_file in data_file_list:
        try:
            dataset =create_dataset(connection=connection, path=data_file)
            dataset.delete()
        except PyBambooException as e:
            outfile.write('pybamboo error: %s\n' % e)
        time.sleep(1)

#_run_with(conn, [water_file, education_file])
#write_to_csv(run_test_suite([water_file, education_file]) )
if __name__ == "__main__":
    outfile = open('logs/%s-%s-%s.log' % (hostname, TESTING, curr_time), 'wb')
    write_to_csv(run_test_suite([water_file]), outfile)
    outfile.close()

