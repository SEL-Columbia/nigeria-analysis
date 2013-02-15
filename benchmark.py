import time
import json
import sys
import os

from pybamboo.dataset import Dataset
from pybamboo.connection import Connection
from pybamboo.exceptions import PyBambooException


URL = 'http://localhost:8080'

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
        d['add_5_calculations_1by1_time'] = time_to_add_5_calculations_1by1(dataset)
        #d['add_5_calculations_batch_time'] = time_to_add_5_calculations_batch(dataset)
        d['update_1_time'] = time_to_add_1_update(dataset)
        d['update_5_1by1_time'] = time_to_add_5_update_1by1(dataset)
        d['update_5_batch_time'] = time_to_add_5_update_batch(dataset)
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

def time_to_add_5_calculations_1by1(dataset):
    info = dataset.get_info()
    calcs = ['true = "T"', 'f = "F"', 'one = "1"', 'two = "2"', 'to = "To"']
    # TODO: more sophisticated calculations?
    #for itemkey,item in info['schema']:
    #    if item['simpletype'] == 'float':
    #        calc_col_1 = itemkey
    #        break
    sleep_between_submissions = 0
    calc_col_1 = '_gps_latitude'
    before = time.time()
    for calc in calcs:
        dataset.add_calculation(calc)
        time.sleep(sleep_between_submissions)
    after = time.time()
    return after - before - sleep_between_submissions * len(calcs)

def time_to_add_1_update(dataset):
    # TODO: more sophisticated updates
    update = [{"mylga" : "test"}]
    before = time.time()
    dataset.update_data(update)
    after = time.time()
    return after - before

def time_to_add_5_update_1by1(dataset):
    updates = [{"mylga" : "test"},
               {"mylga" : "test1"},
               {"mylga" : "test2"},
               {"mylga" : "test3"},
               {"mylga" : "test4"}]
    sleep_between_submissions = 1
    before = time.time()
    for update in updates:
        dataset.update_data([update])
        time.sleep(1)
    after = time.time()
    return after - before - sleep_between_submissions * len(updates)

def time_to_add_5_update_batch(dataset):
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

if __name__ == "__main__":
    valid_test_sizes = ['1', '10', '100', '1000', '10000', '100000']
    test_sizes = sys.argv[1:]
    invalid_test_size = len(test_sizes) == 0 or len([x for x in test_sizes\
            if x not in valid_test_sizes]) > 1

    # choosing between 100, 1000, 10000, 100000
    if invalid_test_size: 
        print """provide size of datasets, like one of:
                   python benchmark.py 1000
                   python benchmark.py 100 1000 10000
                Allowed test sizes are %s
             """ % " ".join(valid_test_sizes) 
        sys.exit()

    DIR = 'csvs/'
    # test on upload
    curr_time = time.strftime('%Y-%m-%d-%H-%M-%S')
    hostname = os.uname()[1]
    outfile = open('logs/%s-%s.log' % (hostname, curr_time), 'wb')
    for test_size in test_sizes:
        test_str = str(test_size)
        water_file = DIR + test_size + '/water.csv'
        education_file = DIR + test_size + '/education.csv'

        #write_to_csv(run_test_suite([water_file, education_file]) )
        write_to_csv(run_test_suite([water_file]), outfile)
    outfile.close()

