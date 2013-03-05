import csv
import itertools
import time
import json
import sys
import os

from pybamboo.dataset import Dataset
from pybamboo.connection import Connection
from pybamboo.exceptions import PyBambooException


URL = 'http://localhost:8080'

def print_that_function_is_running(func):
    """ If you put this decorator around a function, you'll see in stdout when
        func starts running. """
    def wrapped_func(*args, **kwargs):
        print "Running function: ", func.__name__
        return func(*args, **kwargs)
    return wrapped_func

def run_test_suite(dataset_file_path_list):
    print "running test suite for %s" % " ".join(dataset_file_path_list)
    alldata = []
    for dataset_name in dataset_file_path_list:
        d = {}
        d['hostname'] = os.uname()[1]
        d['bamboo_url'] = URL
        d['unix_time'] = time.time()
        conn = Connection(url=URL)
        d['commit'] = conn.version['commit']
        d['branch'] = conn.version['branch']
        dataset = Dataset(connection=conn, path=dataset_name)
        d['import_time'] = time_till_import_is_finished(dataset)
        info = dataset.get_info()
        d['row'] = info['num_rows']
        d['col'] = info['num_columns']
        d['add_1_calculations_time'] = time_to_add_1_calculations(dataset)
        d['add_5_calculations_1by1_time'] = time_to_add_5_calculations_1by1(dataset)
        d['add_5_calculations_batch_time'] = time_to_add_5_calculations_batch(dataset)
        d['update_1_time'] = time_to_add_1_update(dataset)
        d['update_5_1by1_time'] = time_to_add_5_update_1by1(dataset)
        d['update_5_batch_time'] = time_to_add_5_update_batch(dataset)
        dataset.delete()
        alldata.append(d)
    return alldata

def read_from_csv(path):
    row_data = []
    with open(path, 'rb') as csv_file:
        dict_reader = csv.DictReader(csv_file)
        for line in dict_reader:
            row_data.append(line)
    return row_data

def write_to_csv(dict_list, path):
    keys = set(itertools.chain(*dict_list))
    with open(path, 'wb') as csv_file:
        dict_writer = csv.DictWriter(csv_file, keys)
        dict_writer.writeheader()
        for d in dict_list:
            dict_writer.writerow(d)

@print_that_function_is_running
def time_to_add_1_calculations(dataset):
    info = dataset.get_info()
    calc_col_1 = '_gps_latitude'
    before = time.time()
    #TODO: calculation relevant to dataset
    dataset.add_calculation('true = "True"')
    after = time.time()
    return after - before

@print_that_function_is_running
def time_to_add_5_calculations_1by1(dataset):
    info = dataset.get_info()
    calcs = ['true = "T"', 'f = "F"', 'one = "1"', 'two = "2"', 'to = "To"']
    # TODO: more sophisticated calculations?
    sleep_between_submissions = 1
    calc_col_1 = '_gps_latitude'
    before = time.time()
    for calc in calcs:
        dataset.add_calculation(calc)
        time.sleep(sleep_between_submissions)
    after = time.time()
    return after - before - sleep_between_submissions * len(calcs)

@print_that_function_is_running
def time_to_add_5_calculations_batch(dataset):
    info = dataset.get_info()
    calcs = ['t2 = "T"', 'f2 = "F"', 'one2 = "1"', 'two2 = "2"', 'to2 = "To"']
    # TODO: more sophisticated calculations?
    before = time.time()
    dataset.add_calculations(calcs)
    after = time.time()
    return after - before

@print_that_function_is_running
def time_to_add_1_update(dataset):
    # TODO: more sophisticated updates
    update = [{"mylga" : "test"}]
    before = time.time()
    dataset.update_data(update)
    after = time.time()
    return after - before

@print_that_function_is_running
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

@print_that_function_is_running
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

@print_that_function_is_running
def time_till_import_is_finished(dataset):
    before = time.time()
    summary = dataset.get_summary()
    while 'error' in summary.keys():
        time.sleep(1)
        summary = dataset.get_summary()
    after = time.time()
    return after - before

if __name__ == "__main__":
    valid_test_sizes = ['1', '10', '100', '1000', '10000', '100000']
    num_iterations = int(sys.argv[1])
    test_sizes = sys.argv[2:]
    invalid_test_size = len(test_sizes) == 0 or len([x for x in test_sizes\
            if x not in valid_test_sizes]) > 1

    # choosing between 100, 1000, 10000, 100000
    if invalid_test_size:
        print """provide number_iterations, and size of datasets, like one of:
                   python benhcmark.py num_iter dataset_size1 dataset_size2 dataset_size3 ..
                   python benchmark.py 1 1000 # 1 iteration; 1000 row dataset
                   python benchmark.py 3 100 1000 10000 # 3 iterations; 100, 1000, 10000 row datasets
                Allowed test sizes are %s
             """ % " ".join(valid_test_sizes)
        sys.exit()

    DIR = 'csvs/'
    # test on upload
    benchmarkfile = 'benchmark.csv'
    try:
        with open(benchmarkfile) as f: pass
    except IOError as e:
        with open(benchmarkfile, 'wb') as f: f.write('')
    results = read_from_csv(benchmarkfile)
    for i in xrange(1,num_iterations):
        random.shuffle(test_sizes)
        for test_size in test_sizes:
            test_str = str(test_size)
            water_file = DIR + test_size + '/water.csv'
            education_file = DIR + test_size + '/education.csv'
            new_dict = run_test_suite([water_file])
            results += new_dict
            write_to_csv(results, benchmarkfile) # write immediately
            time.sleep(30)
