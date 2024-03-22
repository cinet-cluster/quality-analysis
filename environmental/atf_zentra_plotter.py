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


device_information = [
    {"label": "S-148-ATF", "device_sn": "06-01888", "plot": "atf_soil",
        "clowder_dsid": "630fc714e4b0787c2bfe7cea", "clowder_foid": "630fc72ee4b0787c2bfe7cf3"},
    {"label": "S-151-FF1", "device_sn": "06-01890", "plot": "atmos",
        "clowder_dsid": "630fbbd9e4b0787c2bfe7bfd", "clowder_foid": "630fbbf0e4b0787c2bfe7c03"},
    {"label": "S-151-FF2", "device_sn": "06-01896", "plot": "5port_soil",
        "clowder_dsid": "630fc885e4b0787c2bfe7d13", "clowder_foid": "630fc89be4b0787c2bfe7d1c"},
    {"label": "S-149-RB1", "device_sn": "06-01883", "plot": "atmos",
        "clowder_dsid": "630fb9d6e4b0787c2bfe7bb9", "clowder_foid": "630fba1ee4b0787c2bfe7bc3"},
    {"label": "S-149-RB2", "device_sn": "06-01880", "plot": "5port_soil",
        "clowder_dsid": "630fc7d4e4b0787c2bfe7cfa", "clowder_foid": "630fc7eae4b0787c2bfe7d02"},
    {"label": "S-153-SRFP2", "device_sn": "06-01902", "plot": "srfp2",
        "clowder_dsid": "630fbd07e4b0787c2bfe7c13", "clowder_foid": "630fcce5e4b0787c2bfe7d77"},
]

def generate_atmos_plots(fig, df):
    plots = [{
        "title": "Wind Speed & Direction",
        "x": "Timestamps",
        "y": [
            {"field": "Wind Direction", "label": "Degrees", "color": "grey"},
            {"field": "Wind Speed", "label": "m/s", "color": "black"}
        ]
    }, {
        "title": "Solar Radiation & Temp",
        "x": "Timestamps",
        "y": [
            {"field": "Solar Radiation", "label": "W/m2", "color": "green"},
            {"field": "Air Temperature", "label": "Degrees C", "color": "grey"}
        ]
    }, {
        "title": "Precipitation",
        "x": "Timestamps",
        "y": [
            {"field": "Precipitation", "label": "Millimeters", "color": "blue"}
        ]
    }, {
        "title": "Relative Humidity & VPD",
        "x": "Timestamps",
        "y": [
            {"field": "Relative Humidity", "label": "kPA", "color": "red"},
            {"field": "VPD", "color": "orange"}
        ],
        "share_axis": True
    }, {
        "title": "Leaf Wetness",
        "x": "Timestamps",
        "y": [
            {"field": "Leaf Wetness", "label": "Minutes", "color": "pink"},
            {"field": "Wetness Level", "label": "Counts", "color": "purple"},
        ]
    }]

    subidx = 1
    for plot in plots:
        ax = fig.add_subplot(5, 2, subidx)
        plt.title(plot['title'])
        fig.set_figheight(25)
        fig.set_figwidth(15)
        Axis.set_major_formatter(ax.xaxis, DateFormatter("%m-%d"))

        for series in plot['y']:
            if 'share_axis' not in plot:
                ax = ax.twinx()
            if 'label' in series:
                ax.set_ylabel(series['label'])
            df.plot(ax=ax, x=plot['x'], y=series['field'], color=series['color'], linewidth=1)

        subidx += 1

def generate_atf_soil_plots(fig, df):
    plots = [{
        "title": "Water Content",
        "x": "Timestamps",
        "y": [
            {"field": "Water Content", "label": "Water Content m3/m3", "color": "blue"},
            {"field": "Water Content Port1", "color": "blue"},
            {"field": "Water Content Port2", "color": "teal"},
            {"field": "Water Content Port3", "color": "green"},
        ]
    }, {
        "title": "EC",
        "x": "Timestamps",
        "y": [
            {"field": "Saturation Extract EC", "label": "EC mS/cm", "color": "blue"},
            {"field": "Saturation Extract EC Port1", "color": "blue"},
            {"field": "Saturation Extract EC Port2", "color": "teal"},
            {"field": "Saturation Extract EC Port3", "color": "green"},
        ]
    }, {
        "title": "Soil Temp",
        "x": "Timestamps",
        "y": [
            {"field": "Soil Temperature", "label": "Soil Temp C", "color": "blue"},
            {"field": "Soil Temperature Port1", "color": "blue"},
            {"field": "Soil Temperature Port2", "color": "teal"},
            {"field": "Soil Temperature Port3", "color": "green"},
        ]
    }]

    subidx = 1
    for plot in plots:
        ax = fig.add_subplot(3, 1, subidx)
        plt.title(plot['title'])
        fig.set_figheight(25)
        fig.set_figwidth(15)
        Axis.set_major_formatter(ax.xaxis, DateFormatter("%m-%d"))

        for series in plot['y']:
            if 'label' in series:
                ax.set_ylabel(series['label'])
            df.plot(ax=ax, x=plot['x'], y=series['field'], color=series['color'], linewidth=1)

        subidx += 1

def generate_5port_soil_plots(fig, df):
    plots = [{
        "title": "Water Content",
        "x": "Timestamps",
        "y": [
            {"field": "Water Content", "label": "Water Content m3/m3", "color": "blue"},
            {"field": "Water Content Port1", "color": "blue"},
            {"field": "Water Content Port2", "color": "teal"},
            {"field": "Water Content Port3", "color": "darkgreen"},
            {"field": "Water Content Port4", "color": "green"},
            {"field": "Water Content Port5", "color": "orange"},
        ]
    }, {
        "title": "EC",
        "x": "Timestamps",
        "y": [
            {"field": "Saturation Extract EC", "label": "EC mS/cm", "color": "blue"},
            {"field": "Saturation Extract EC Port1", "color": "blue"},
            {"field": "Saturation Extract EC Port2", "color": "teal"},
            {"field": "Saturation Extract EC Port3", "color": "darkgreen"},
            {"field": "Saturation Extract EC Port4", "color": "green"},
            {"field": "Saturation Extract EC Port5", "color": "orange"},
        ]
    }, {
        "title": "Soil Temp",
        "x": "Timestamps",
        "y": [
            {"field": "Soil Temperature", "label": "Soil Temp C", "color": "blue"},
            {"field": "Soil Temperature Port1", "color": "blue"},
            {"field": "Soil Temperature Port2", "color": "teal"},
            {"field": "Soil Temperature Port3", "color": "darkgreen"},
            {"field": "Soil Temperature Port4", "color": "green"},
            {"field": "Soil Temperature Port5", "color": "orange"},
        ]
    }]

    subidx = 1
    for plot in plots:
        ax = fig.add_subplot(3, 1, subidx)
        plt.title(plot['title'])
        fig.set_figheight(25)
        fig.set_figwidth(15)
        Axis.set_major_formatter(ax.xaxis, DateFormatter("%m-%d"))

        for series in plot['y']:
            if 'label' in series:
                ax.set_ylabel(series['label'], color=series['color'])
            df.plot(ax=ax, x=plot['x'], y=series['field'], color=series['color'], linewidth=1)

        subidx += 1

def generate_srfp2_plots(fig, df):
    plots = [{
        "title": "Solar Radiation & Temp",
        "x": "Timestamps",
        "y": [
            {"field": "Solar Radiation", "label": "W/m2", "color": "grey"},
            {"field": "Air Temperature", "label": "Temperature C", "color": "green"},
        ]
    }, {
        "title": "Precipitation mm",
        "x": "Timestamps",
        "y": [
            {"field": "Precipitation", "label": "Millimeters", "color": "blue"},
        ]
    }, {
        "title": "Atmospheric Pressure",
        "x": "Timestamps",
        "y": [
            {"field": "Relative Humidity", "label": "kPA", "color": "red"},
            {"field": "VPD", "color": "orange"}
        ],
        "share_axis": True
    }, {
        "title": "Wind Speed & Direction",
        "x": "Timestamps",
        "y": [
            {"field": "Wind Speed", "label": "m/s", "color": "grey"},
            {"field": "Wind Direction", "label": "Degrees", "color": "black"},
        ]
    }]

    subidx = 1
    for plot in plots:
        ax = fig.add_subplot(4, 1, subidx)
        plt.title(plot['title'])
        fig.set_figheight(25)
        fig.set_figwidth(15)
        Axis.set_major_formatter(ax.xaxis, DateFormatter("%m-%d"))

        for series in plot['y']:
            if 'share_axis' not in plot:
                ax = ax.twinx()
            if 'label' in series:
                ax.set_ylabel(series['label'])
            df.plot(ax=ax, x=plot['x'], y=series['field'], color=series['color'], linewidth=1)

        subidx += 1

def upload_results(device, filename):
    # Upload file to Clowder dataset
    clowder_url = "http://cinet.ncsa.illinois.edu/clowder/api/"
    key = ""  # Replace this with a valid Clowder API key (create in user profile)
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
for device in device_information:
    target_filename = f"{device['label']}_{curr_year}_Aggregate.xlsx"

    # Load data & filter to last month
    print("Plotting %s" % target_filename)
    df = pd.read_excel(target_filename)
    df['Timestamps'] = pd.to_datetime(df['Timestamps'])
    date_val = (datetime.datetime.now() - datetime.timedelta(days=14)).astimezone()

    # Get the first day of current month
    date_val = datetime.datetime.today().replace(day=1).astimezone()
    df = df[df['Timestamps'] > date_val]

    print("Generating plots...")
    fig = plt.figure()
    if device['plot'] == 'atmos':
        generate_atmos_plots(fig, df)
    elif device['plot'] == 'atf_soil':
        generate_atf_soil_plots(fig, df)
    elif device['plot'] == '5port_soil':
        generate_5port_soil_plots(fig, df)
    elif device['plot'] == 'srfp2':
        generate_srfp2_plots(fig, df)

    outfile = "plots/%s_%s.pdf" % (device['label'], out_month)
    plt.savefig(outfile, format="pdf", bbox_inches="tight")
    upload_results(device, outfile)

print("Done.")
