import os
import json
import requests
import time
from datetime import datetime
from io import StringIO
import pandas as pd
from pyclowder import files as pyfiles
from pyclowder.connectors import Connector

conn = Connector("", {})
base_url = "https://zentracloud.com/api/v4/get_readings"
api_token = "Token 12345"  # Replace this with a valid token from the ZC API
device_information = [
    {"label": "S-148-ATF", "device_sn": "06-01888", "clowder_dsid": "630fc714e4b0787c2bfe7cea"},
    {"label": "S-151-FF1", "device_sn": "06-01890", "clowder_dsid": "630fbbd9e4b0787c2bfe7bfd"},
    {"label": "S-151-FF2", "device_sn": "06-01896", "clowder_dsid": "630fc885e4b0787c2bfe7d13"},
    {"label": "S-149-RB1", "device_sn": "06-01883", "clowder_dsid": "630fb9d6e4b0787c2bfe7bb9"},
    {"label": "S-149-RB2", "device_sn": "06-01880", "clowder_dsid": "630fc7d4e4b0787c2bfe7cfa"},
    {"label": "S-153-SRFP2", "device_sn": "06-01902", "clowder_dsid": "630fbd07e4b0787c2bfe7c13"},
]


def upload_results(device, filename):
    # Upload file to Clowder dataset
    clowder_url = "http://cinet.ncsa.illinois.edu/clowder/api/"
    key = ""  # Replace this with a valid Clowder API key (create in user profile)
    dataset_id = device["clowder_dsid"]

    print("Uploading %s to %s" % (filename, dataset_id))

    files = [('File', open(filename, 'rb'))]
    #r = pyfiles.upload_to_dataset(conn, clowder_url, key, dataset_id, filename)
    r = requests.post(f"{clowder_url}datasets/{dataset_id}/files",
                      files=files, headers={'X-API-key': key}, verify=False)
    new_file_id = r.json()['id']

    # Remove any older copies from dataset upon success
    if new_file_id is not None:
        file_list = requests.get(f"{clowder_url}datasets/{dataset_id}/files",
                                 headers={'X-API-key': key, 'Content-type': 'application/json'}, verify=False).json()
        for f in file_list:
            if f['filename'] == filename and f['id'] != new_file_id:
                requests.delete(f"{clowder_url}files/{f['id']}", headers={'X-API-key': key}, verify=False)


now = datetime.now()
curr_year = "2024"  # now.strftime("%Y")
curr_time = now.strftime("%m-%d-%Y %H%M%S")
for device in device_information:
    print(f"--{device['label']}--")
    output_filename = f"{device['label']}_{curr_year}_Aggregate.xlsx"

    # Determine last timestamp
    df = pd.read_excel(output_filename)
    df['Timestamps'] = df['Timestamps'].astype(str)
    earliest = df['Timestamps'].iloc[0]
    latest = df['Timestamps'].iloc[-1]
    print(f"Found previous results from {earliest} to {latest}")

    # By default, this will start at most recent page of records and walk backwards
    params = {'device_sn': device['device_sn'],
              'per_page': 2000}
    if latest is not None:
        params['start_date'] = latest

    try:
        response = requests.get(base_url, params=params,
                            headers={'content-type': 'application/json', 'Authorization': api_token})
    except Exception as e:
        print(e)
        print("Initial request failed, skipping.")
        continue
    response.raise_for_status()
    content = json.loads(response.content)
    next_url = content['pagination']['next_url']
    page_end = content['pagination']['page_end_date']

    # Reformat the results and load into a dataframe
    readings = {}
    for measure in content['data']:
        multi_ports = True if len(content['data'][measure]) > 1 else False
        for measure_entry in content['data'][measure]:
            meta = measure_entry['metadata']
            # Some measures can repeat across ports
            measure_name = measure if not multi_ports else f"{measure} Port{meta['port_number']}"
            for reading in measure_entry['readings']:
                dt = reading['datetime']
                if dt not in readings:
                    readings[dt] = {'Timestamps': dt}
                readings[dt][measure_name] = reading['value']
    new_df = pd.read_json(StringIO(json.dumps(readings)), orient='index')
    try:
        new_df['Timestamps'] = new_df['Timestamps'].astype(str)
    except KeyError:
        print("No Timestamps found, breaking.")
        continue

    # ExcelWriter doesn't provide header skip control so do it manually
    df = pd.concat([df, new_df], ignore_index=True)
    df = df.sort_values(by='Timestamps')
    df.to_excel(output_filename, index=False)

    while next_url is not None:
        """
        Continue to fetch pages as long as necessary, really only needed when filling big gaps.
        Users are limited to a total of 60 calls per minute, and each device is limited to 1 call per minute. 
        One user can make 60 calls to 60 different devices in a 60 second period.
        """
        if page_end[:10] >= str(datetime.now())[:10]:
            print("All caught up.")
            break

        time.sleep(61)
        retries = 0
        print(next_url)
        try:
            response = requests.get(next_url,
                                    headers={'content-type': 'application/json', 'Authorization': api_token})
            response.raise_for_status()
        except Exception as e:
            retries += 1
            if retries > 5:
                print("Aborting calls to this device for now.")
                next_url = None
                continue
            print("Failed to connect, retrying in 60s...")
            continue

        content = json.loads(response.content)
        next_url = content['pagination']['next_url']
        page_end = content['pagination']['page_end_date']

        # Reformat the results and load into a dataframe
        readings = {}
        for measure in content['data']:
            for measure_entry in content['data'][measure]:
                meta = measure_entry['metadata']
                for reading in measure_entry['readings']:
                    dt = reading['datetime']
                    if str(dt) != 'nan':
                        if dt not in readings:
                            readings[dt] = {'Timestamps': dt}
                        readings[dt][measure] = reading['value']
        new_df = pd.read_json(StringIO(json.dumps(readings)), orient='index')
        try:
            new_df['Timestamps'] = new_df['Timestamps'].astype(str)
        except KeyError:
            print("No Timestamps found, breaking.")
            break


        df = pd.concat([df, new_df], ignore_index=True)
        df = df.sort_values(by='Timestamps')
        df['Timestamps'] = df['Timestamps'].astype(str)
        df.to_excel(output_filename, index=False)

    upload_results(device, output_filename)

print("All done.")
