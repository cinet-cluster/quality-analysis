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


data_dir = "/Users/mburnette/Git/quality-analysis/mirz"
ILAG_prefix = "ILAG_ATF_MIRZ"

def generate_ilag_plots(fig, df):
    """
    Four depths of raw CO2

    AvgRawCO2_110cm
AvgRawCO2_180cm

AvgRawCO2_60cm

    Four depths of raw O2
    “” for volumetric water content
    VWC sensors - plot their temp data (x4)
    20, 60, 110, 180cm
    Electrical connectivity data
    O2, CO2 temp data is meant as a correction, not as environmental data source.
    Everything at 20cm on one plot, same for 60, same for 80
    In addition to the plots above
    Four axis map!
    """
    plots = [{
        "title": "Raw CO2",
        "x": "Timestamps",
        "y": [
            {"field": "AvgRawCO2_20cm", "label": "20cm", "color": "green"},
            {"field": "AvgRawCO2_60cm", "label": "60cm", "color": "yellow"},
            {"field": "AvgRawCO2_110cm", "label": "110cm", "color": "orange"},
            {"field": "AvgRawCO2_180cm", "label": "180cm", "color": "red"},
        ],
        "share_axis": True
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
    if f.startswith(ILAG_prefix):
        target_filename = os.path.join(data_dir, f)
        print("Plotting %s" % target_filename)
        df = pd.read_excel(target_filename, header=1, skiprows=[2,3])

        # fix erroneous rows
        df['Timestamps'] = pd.to_datetime(df['TIMESTAMP']).dt.date

        # Filter to current month
        #date_val = (datetime.datetime.now() - datetime.timedelta(days=14)).astimezone()
        #date_val = datetime.datetime.today().replace(day=1).astimezone()
        #df = df[df['Timestamps'] > date_val]

        print("Generating plots...")
        fig = plt.figure()
        generate_ilag_plots(fig, df)

        outfile = "plots/%s_%s.pdf" % (ILAG_prefix, out_month)
        plt.savefig(outfile, format="pdf", bbox_inches="tight")
        #upload_results(device, outfile)

print("Done.")
