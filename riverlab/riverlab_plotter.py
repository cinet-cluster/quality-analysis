import os
import json
import requests
import time
import datetime
from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.axis import Axis
from matplotlib.dates import DateFormatter


data_dir = "/Users/mburnette/Git/quality-analysis/riverlab"
data_prefix = "Riverlab Data"
chem_prefix = "Riverlab Chemistry Data"

def generate_data_plots(fig, df):
    plots = [{
        "title": "River Level",
        "x": "Timestamps",
        "y": [
            {"field": "Niveau Riviere", "label": "Level", "color": "green"}
        ]
    }, {
        "title": "River Flow",
        "x": "Timestamps",
        "y": [
            {"field": "Pump Flow", "label": "Pump Flow", "color": "orange"}
        ]
    }, {
        "title": "Pressure",
        "x": "Timestamps",
        "y": [
            {"field": "Pression EF", "label": "EF", "color": "blue"},
        ]
    }, {
        "title": "Temperature & Turbidity",
        "x": "Timestamps",
        "y": [
            {"field": "Probe Tank Temperature", "label": "Probe Tank Temp", "color": "red"},
            {"field": "Turbidity", "label": "Turbidity", "color": "blue"}
        ]
    }]

    subidx = 1
    for plot in plots:
        ax = fig.add_subplot(5, 2, subidx)
        plt.title(plot['title'])
        fig.set_figheight(25)
        fig.set_figwidth(25)
        Axis.set_major_formatter(ax.xaxis, DateFormatter("%m-%d"))

        for series in plot['y']:
            if 'share_axis' not in plot:
                ax = ax.twinx()
            if 'label' in series:
                ax.set_ylabel(series['label'])
            try:
                df.plot(ax=ax, x=plot['x'], y=series['field'], color=series['color'], linewidth=1)
            except:
                print("Problem plotting %s" % series['field'])
                continue

        subidx += 1

def generate_chem_plots(fig, df):
    plots = [{
        "title": "Mega Mix",
        "x": "Timestamps",
        "y": [
            {"field": "Calcium", "label": "Calcium", "color": "green"},
            {"field": "Chlorides", "label": "Calcium", "color": "red"},
            {"field": "Fluorides", "label": "Calcium", "color": "orange"},
            {"field": "Lithium", "label": "Calcium", "color": "blue"},
            {"field": "Magnesium", "label": "Calcium", "color": "yellow"},
            {"field": "Nitrates", "label": "Calcium", "color": "red"},
            {"field": "Nitrites", "label": "Calcium", "color": "black"},
            {"field": "Phosphates", "label": "Calcium", "color": "gray"},
            {"field": "Potassium", "label": "Calcium", "color": "pink"},
            {"field": "Silicium", "label": "Calcium", "color": "magenta"},
            {"field": "Sodium", "label": "Calcium", "color": "cyan"},
            {"field": "Sulfates", "label": "Calcium", "color": "purple"},
        ],
        "share_axis": True
    }, {
        "title": "Calcium",
        "x": "Timestamps",
        "y": [
            {"field": "Calcium", "label": "Calcium", "color": "green"}
        ]
    }, {
        "title": "Chlorides",
        "x": "Timestamps",
        "y": [
            {"field": "Chlorides", "label": "Chlorides", "color": "red"}
        ]
    }, {
        "title": "Fluorides",
        "x": "Timestamps",
        "y": [
            {"field": "Fluorides", "label": "Fluorides", "color": "orange"}
        ]
    }, {
        "title": "Lithium",
        "x": "Timestamps",
        "y": [
            {"field": "Lithium", "label": "Lithium", "color": "blue"}
        ]
    }, {
        "title": "Magnesium",
        "x": "Timestamps",
        "y": [
            {"field": "Magnesium", "label": "Magnesium", "color": "yellow"}
        ]
    }, {
        "title": "Nitrates",
        "x": "Timestamps",
        "y": [
            {"field": "Nitrates", "label": "Nitrates", "color": "red"}
        ]
    }, {
        "title": "Nitrites",
        "x": "Timestamps",
        "y": [
            {"field": "Nitrites", "label": "Nitrites", "color": "black"}
        ]
    }, {
        "title": "Phosphates",
        "x": "Timestamps",
        "y": [
            {"field": "Phosphates", "label": "Phosphates", "color": "gray"}
        ]
    }, {
        "title": "Potassium",
        "x": "Timestamps",
        "y": [
            {"field": "Potassium", "label": "Potassium", "color": "pink"}
        ]
    }, {
        "title": "Silicium",
        "x": "Timestamps",
        "y": [
            {"field": "Silicium", "label": "Silicium", "color": "magenta"}
        ]
    }, {
        "title": "Sodium",
        "x": "Timestamps",
        "y": [
            {"field": "Sodium", "label": "Sodium", "color": "cyan"}
        ]
    }, {
        "title": "Sulfates",
        "x": "Timestamps",
        "y": [
            {"field": "Sulfates", "label": "Sulfates", "color": "purple"}
        ]
    }]

    subidx = 1
    for plot in plots:
        ax = fig.add_subplot(14, 2, subidx)
        plt.title(plot['title'])
        fig.set_figheight(50)
        fig.set_figwidth(25)
        Axis.set_major_formatter(ax.xaxis, DateFormatter("%m-%d"))

        for series in plot['y']:
            if 'share_axis' not in plot:
                ax = ax.twinx()
            if 'label' in series:
                ax.set_ylabel(series['label'])
            try:
                df.plot(ax=ax, x=plot['x'], y=series['field'], color=series['color'], linewidth=1)
            except:
                print("Problem plotting %s" % series['field'])
                continue

        subidx += 1

def upload_results(device, filename):
    # Upload file to Clowder dataset
    clowder_url = "http://cinet.ncsa.illinois.edu/clowder/api/"
    key = "123"  # Replace this with a valid Clowder API key (create in user profile)
    dataset_id = device["clowder_dsid"]
    folder_id = device["clowder_foid"]

    print("Uploading %s" % filename)
    files = [('File', open(filename, 'rb'))]
    r = requests.post(f"{clowder_url}uploadToDataset/{dataset_id}?folder_id={folder_id}",
                      files=files, headers={'X-API-key': key}, verify=False)
    new_file_id = r.json()['id']

    # Remove any older copies from dataset upon success
    if new_file_id is not None:
        file_list = requests.get(f"{clowder_url}datasets/{dataset_id}/files",
                                 headers={'X-API-key': key, 'Content-type': 'application/json'}, verify=False).json()
        for f in file_list:
            if f['filename'] == filename and f['id'] != new_file_id:
                requests.delete(f"{clowder_url}files/{f['id']}", headers={'X-API-key': key}, verify=False)


now = datetime.datetime.now()
curr_year = "2024"  # now.strftime("%Y")
out_month = now.strftime("%B") + str(now.year)
for f in os.listdir(data_dir):
    if f.startswith(data_prefix):
        target_filename = os.path.join(data_dir, f)
        print("Plotting %s" % target_filename)
        df = pd.read_csv(target_filename)

        # fix erroneous rows
        df = df.drop(df[df['timedate'] == '67.0'].index)
        df['Timestamps'] = pd.to_datetime(df['timedate'])

        # Filter to current month
        date_val = (datetime.datetime.now() - datetime.timedelta(days=14)).astimezone()
        date_val = datetime.datetime.today().replace(day=1).astimezone()
        df = df[df['Timestamps'] > date_val]

        print("Generating plots...")
        fig = plt.figure()
        generate_data_plots(fig, df)

        outfile = "plots/%s_%s.pdf" % ("Riverlab Data", out_month)
        plt.savefig(outfile, format="pdf", bbox_inches="tight")
        #upload_results(device, outfile)

    elif f.startswith(chem_prefix):
        target_filename = os.path.join(data_dir, f)
        print("Plotting %s" % target_filename)
        df = pd.read_csv(target_filename)

        # fix erroneous rows
        #df = df.drop(df[df['timedate'] == '67.0'].index)
        df['Timestamps'] = pd.to_datetime(df['timedate'])

        # Filter to current month
        date_val = (datetime.datetime.now() - datetime.timedelta(days=14)).astimezone()
        date_val = datetime.datetime.today().replace(day=1).astimezone()
        df = df[df['Timestamps'] > date_val]

        print("Generating plots...")
        fig = plt.figure()
        generate_chem_plots(fig, df)

        outfile = "plots/%s_%s.pdf" % ("Riverlab Chemistry Data", out_month)
        plt.savefig(outfile, format="pdf", bbox_inches="tight")
        #upload_results(device, outfile)


print("Done.")
