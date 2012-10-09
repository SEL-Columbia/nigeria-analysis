import pickle
import simplejson as json

from pybamboo import PyBamboo


pyb = PyBamboo('http://localhost:8080')

nigeria_ids = None
cached_file_name = 'cached_nigeria_ids.p'

try:
    nigeria_ids, merge_id = pickle.load(open(cached_file_name))
except IOError as e:
    nigeria_water_05=pyb.store_csv_file(
        'Water_05_06_2012_2012_10_01_15_00_38.csv')
    nigeria_water_22=pyb.store_csv_file(
        'Water_22_05_2012_2012_10_01_15_01_39.csv')
    nigeria_water_24=pyb.store_csv_file(
        'Water_24_04_2012_2012_10_01_15_03_51.csv')
    nigeria_ids = [
        nigeria_water_05['id'],
        nigeria_water_22['id'],
        nigeria_water_24['id'],
    ]

    # merge them all
    response = pyb.merge(nigeria_ids)
    merge_id = response['id']
    pickle.dump([nigeria_ids, merge_id], open(cached_file_name, 'wb'))

print 'merge_id: %s' % merge_id

info = pyb.info(merge_id)
schema = info['schema'].keys()
schema.sort()
print json.dumps(schema, indent=4, sort_keys=True)

def store_calc(name, formula):
    response = pyb.store_calculation(merge_id, name, formula, 'mylga_state')
    print response

if True:
    store_calc(
        'proportion_boreholes',
        'ratio(water_source_type in ["borehole_tube_well"], 1)')

    store_calc(
        'proportion_bh_functional',
        'ratio(water_source_type in ["borehole_tube_well"] and water_functiona'
        'l_yn in ["yes"], water_source_type in ["borehole_tube_well"])')

    store_calc(
        'proportion_bh_motorized',
        'ratio(water_source_type in ["borehole_tube_well"] and lift_mechanism '
        'in ["fuel_pump", "solar_pump", "electricity_pump", "wind_pump"] and'
        ' water_functional_yn in ["yes"], water_source_type in ["borehole_tube'
        '_well"])')

import requests
print requests.get('%s?mode=related' % pyb.get_dataset_url(merge_id)).text
