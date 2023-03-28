import csv
import json

csv_file = 'ugigeos_data.csv' # This is downloaded from https://github.com/CONCYTEC/ubigeo-peru/blob/master/equivalencia-ubigeos-oti-concytec.csv

def process_csv_data(csv_file):
    ubigeo_data = {}

    with open(csv_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        
        for row in reader:
            department_code, department_name, province_code, province_name, district_code, district_name, *remain = row

            if department_code not in ubigeo_data:
                ubigeo_data[department_code] = {
                    'department': department_name,
                    'provinces': {}
                }

            if province_code not in ubigeo_data[department_code]['provinces']:
                ubigeo_data[department_code]['provinces'][province_code] = {
                    'province': province_name,
                    'districts': {}
                }

            ubigeo_data[department_code]['provinces'][province_code]['districts'][district_code] = {
                'district': district_name
            }
    return ubigeo_data

def convert_to_json_structure(ubigeo_data):
    data = {'Peru': []}

    for department_code, department_data in ubigeo_data.items():
        department = {
            'department': department_data['department'],
            'code': department_code,
            'provinces': []
        }

        for province_code, province_data in department_data['provinces'].items():
            province = {
                'province': province_data['province'],
                'code': province_code,
                'districts': []
            }

            for district_code, district_data in province_data['districts'].items():
                district = {
                    'district': district_data['district'],
                    'code': district_code
                }
                province['districts'].append(district)

            department['provinces'].append(province)

        data['Peru'].append(department)
    return data

ubigeo_data = process_csv_data(csv_file)
json_data = convert_to_json_structure(ubigeo_data)

with open('ubigeos_data.json', 'w') as jsonfile:
    json.dump(json_data, jsonfile, indent=2)
