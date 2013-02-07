import json
import sys
import time

from pybamboo.connection import Connection
from pybamboo.dataset import Dataset


PROD_BAMBOO_ID_FILE = 'ids/ids.prod.json'
DEV_BAMBOO_ID_FILE = 'ids/ids.dev.json'
DEV_BAMBOO = False

# set bamboo instance
if DEV_BAMBOO:
    connection = Connection('http://localhost:8080')
    bamboo_id_file = DEV_BAMBOO_ID_FILE
else:
    connection = Connection()
    bamboo_id_file = PROD_BAMBOO_ID_FILE

# get state of current datasets
with open(bamboo_id_file) as f:
    bamboo_ids = json.loads(f.read())
if not bamboo_ids:
    print '"%s" not found: exiting' % bamboo_id_file
    sys.exit(0)
print 'current dataset status:'
print json.dumps(bamboo_ids, indent=4, sort_keys=True)

# upload originals
for sector in bamboo_ids.keys():
    for name, id in bamboo_ids[sector]['originals'].iteritems():
        if not id:
            print 'dataset: %s not uploaded, uploading %s.csv' % (name, name)
            dataset = Dataset(connection=connection, path='csvs/originals/%s.csv' % name)
            state = dataset.get_info()['state']
            while state != 'ready':
                time.sleep(1)
                state = dataset.get_info()['state']
                print state
            bamboo_ids[sector]['originals'][name] = dataset.id
            with open(bamboo_id_file, 'wb') as f:
                f.write(json.dumps(bamboo_ids))

# merge originals
for sector in bamboo_ids.keys():
    if not bamboo_ids[sector]['merged']:
        print 'no merged dataset for sector: %s' % sector
        datasets = [Dataset(connection=connection, dataset_id=id) for name, id
            in bamboo_ids[sector]['originals'].iteritems()]
        print 'merging datasets: %s' % [dataset.id for dataset in datasets]
        merged = Dataset.merge(datasets, connection=connection)
        print 'merged: %s' % merged
        state = merged.get_info()['state']
        while state != 'ready':
            time.sleep(1)
            state = merged.get_info()['state']
        bamboo_ids[sector]['merged'] = merged.id
        with open(bamboo_id_file, 'wb') as f:
            f.write(json.dumps(bamboo_ids))

# add calculations
print "starting: add calculations"
for sector in bamboo_ids.keys():
    print "adding calculation for sector %s" % sector
    calculations = []
    with open('calculations/%s.txt' % sector) as f:
        calculations = f.readlines()
    print "done reading file for sector %s" % sector
    dataset = Dataset(connection=connection,
        dataset_id=bamboo_ids[sector]['merged'])
    print "receiving dataset id %s" % dataset.id
    for calculation in calculations:
        print "existence check for calculations"
        existing_calculations = [d['name'] for d in dataset.get_calculations()]
        name = calculation.split('=')[0]
        print "calculation %s is being checked" % name
        print "existing calculations are %s" % existing_calculations
        if name not in existing_calculations:
            print "start adding calculation %s" % name
            added = dataset.add_calculation(calculation)
            print 'added calculation: %s (%s)' % (name, added)
        else:
            print 'calculation: %s already exists' % name

# add aggregations
