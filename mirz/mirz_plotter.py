import requests
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd


data_dir = "/Users/mburnette/Git/quality-analysis/mirz"
MY_API_KEY = '271e28cb-347c-4628-96b5-9c5e75db8dcf'  # Replace with valid Clowder API key
clowder_base_uri = "https://cinet.ncsa.illinois.edu/clowder/api"  # CINet

mirz_dataset = '61571c894f0ca77f5a4badfa'
src_folders = {
    "ILAG": "6283f7a3e4b008bf7ef88979",
    "ILPR": "62262d94e4b024d4c188fcb4",
    "NEAG": "64fb20ebe4b0e0dcd6752860",
    "NEPR": "62262df4e4b024d4c188fcd0",
}
dest_folders = {
    "ILAG": "663e24ebe4b03e4b1b789f90",
    "ILPR": "663e2512e4b03e4b1b789fb6",
    "NEAG": "",
    "NEPR": "",
}
file_prefix = {
    "ILAG": "ILAG_ATF_MIRZ_CR1000x_",
    "ILPR": "ILPR_SRFP_MIRZ_CR1000_TEROSdata_",
    "NEAG": "",
    "NEPR": "",
}
clowder_repository = '6489d855e4b0bcb1fe243179'    # MAX'S DATASET

base_headers = {'X-API-key':MY_API_KEY}
headers = {**base_headers, 'Content-type': 'application/json',
           'accept': 'application/json'}


def list_files_of_dataset(dataset_id, folder_id=None):
    r = requests.get(clowder_base_uri + '/datasets/' + dataset_id + '/files',
                     headers=headers, verify=False)
    if r.status_code == 200:
        if folder_id is not None:
            return r.json()
        else:
            cleaned = []
            for f in r.json():
                if 'folders' in f and f['folders']['id'] == folder_id:
                    cleaned.append(f)
            return cleaned
    else:
        return r.text

def generate_il_plots(fig, df):
    """
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
    }, {
        "title": "Raw O2",
        "x": "Timestamps",
        "y": [
            {"field": "AveRaw_O2_20cm", "label": "20cm", "color": "green"},
            {"field": "AveRaw_O2_60cm", "label": "60cm", "color": "yellow"},
            {"field": "AveRaw_O2_110cm", "label": "110cm", "color": "orange"},
            {"field": "AveRaw_O2_180cm", "label": "180cm", "color": "red"},
        ],
        "share_axis": True
    }, {
        "title": "VWC Dielectric",
        "x": "Timestamps",
        "y": [
            {"field": "VWCdielectric_20cm_Avg", "label": "20cm", "color": "green"},
            {"field": "VWCdielectric_60cm_Avg", "label": "60cm", "color": "yellow"},
            {"field": "VWCdielectric_110cm_Avg", "label": "110cm", "color": "orange"},
            {"field": "VWCdielectric_180cm_Avg", "label": "180cm", "color": "red"},
        ],
        "share_axis": True
    }, {
        "title": "VWC Mineral",
        "x": "Timestamps",
        "y": [
            {"field": "VWCmineral_20cm_Avg", "label": "20cm", "color": "green"},
            {"field": "VWCmineral_60cm_Avg", "label": "60cm", "color": "yellow"},
            {"field": "VWCmineral_110cm_Avg", "label": "110cm", "color": "orange"},
            {"field": "VWCmineral_180cm_Avg", "label": "180cm", "color": "red"},
        ],
        "share_axis": True
    }, {
        "title": "Electrical Conductivity",
        "x": "Timestamps",
        "y": [
            {"field": "electricalConductivity_20cm_Avg", "label": "20cm", "color": "green"},
            {"field": "electricalConductivity_60cm_Avg", "label": "60cm", "color": "yellow"},
            {"field": "electricalConductivity_110cm_Avg", "label": "110cm", "color": "orange"},
            {"field": "electricalConductivity_180cm_Avg", "label": "180cm", "color": "red"},
        ],
        "share_axis": True
    }]

    subidx = 1
    for plot in plots:
        ax = fig.add_subplot(5, 2, subidx)
        ax.ticklabel_format(useOffset=False)
        plt.title(plot['title'])
        fig.set_figheight(50)
        fig.set_figwidth(25)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ys = []
        for series in plot['y']:
            ys.append(series['field'])
        df.plot(ax=ax, x=plot['x'], y=ys, color=['green', 'yellow', 'orange', 'red'], linewidth=1,
                use_index=True)
        subidx += 1

    stack_plots = []
    for cm in ["20cm", "60cm", "110cm", "180cm"]:
        stack_plots.append({
            "title": f"{cm} Readings",
            "x": "Timestamps",
            "y": [
                {"field": f"AvgRawCO2_{cm}", "label": "AvgRawCO2", "color": "green"},
                {"field": f"AveRaw_O2_{cm}_x100", "label": "AveRaw_O2", "color": "yellow"},
                {"field": f"VWCdielectric_{cm}_Avg_x100", "label": "VWCdielectric", "color": "orange"},
                {"field": f"VWCmineral_{cm}_Avg_x10000", "label": "VWCmineral", "color": "red"},
                {"field": f"electricalConductivity_{cm}_Avg_x10", "label": "electricalConductivity", "color": "blue"},
            ]
        })

    for plot in stack_plots:
        ax = fig.add_subplot(5, 2, subidx)
        ax.ticklabel_format(useOffset=False)
        plt.title(plot['title'])
        fig.set_figheight(50)
        fig.set_figwidth(25)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ys = []
        for series in plot['y']:
            ys.append(series['field'])
        df.plot(ax=ax, x=plot['x'], y=ys, color=['green', 'yellow', 'orange', 'red', 'blue'], linewidth=1,
                use_index=True)
        subidx += 1

def upload_results(src, filename):
    # Upload file to Clowder dataset
    clowder_url = "http://cinet.ncsa.illinois.edu/clowder/api/"
    folder_id = dest_folders[src]

    print("Uploading %s" % filename)
    files = [('File', open(filename, 'rb'))]
    r = requests.post(f"{clowder_url}uploadToDataset/{mirz_dataset}?folder_id={folder_id}",
                      files=files, headers={'X-API-key': MY_API_KEY}, verify=False)
    new_file_id = r.json()['id']

    # Remove any older copies from dataset upon success
    if new_file_id is not None:
        file_list = requests.get(f"{clowder_url}datasets/{mirz_dataset}/listAllFiles",
                                 headers={'X-API-key': MY_API_KEY, 'Content-type': 'application/json'}, verify=False).json()
        for f in file_list:
            if f['filename'] == filename and f['id'] != new_file_id:
                requests.delete(f"{clowder_url}files/{f['id']}", headers={'X-API-key': MY_API_KEY}, verify=False)


now = datetime.datetime.now()
for src in ["ILAG", "ILPR"]:
    folder = src_folders[src]
    mirz_files = list_files_of_dataset(mirz_dataset, folder)
    excel_files = []
    for f in mirz_files:
        if f['filename'].startswith(file_prefix[src]) and f['filename'].endswith('.xlsx'):
            excel_files.append(f)
    pdf_graphs = []
    sub_files = list_files_of_dataset(mirz_dataset, dest_folders[src])
    for f in sub_files:
        if f['filename'].startswith(file_prefix[src]) and f['filename'].endswith('.pdf'):
            pdf_graphs.append(f['filename'])


    for f in excel_files:
        destfile = f['filename'].replace(".xlsx", ".pdf")
        if destfile in pdf_graphs:
            print("%s already exists" % destfile)
            continue

        print("----------"+f['filename']+"----------")
        url = requests.get(clowder_base_uri + '/files/' + f['id'] + '/blob?key=' + MY_API_KEY, verify=False)
        df = pd.read_excel(url.content, header=1, skiprows=[2, 3])

        # fix erroneous rows
        df['Timestamps'] = pd.to_datetime(df['TIMESTAMP']).dt.date

        # Filter to current month
        #date_val = datetime.datetime.today().replace(month=4, day=1).astimezone().date()
        #df = df[df['Timestamps'] > date_val]
        date_val = datetime.datetime.today().replace(day=1)
        df['Timestamps'] = pd.to_datetime(df['TIMESTAMP'])
        df.set_index('Timestamps')

        fig = plt.figure()
        if src in ["ILAG", "ILPR"]:
            # Derive some new params to plot
            for cm in ["20cm", "60cm", "110cm", "180cm"]:
                if f"AveRaw_O2_{cm}" in df:
                    df[f"AveRaw_O2_{cm}_x100"] = df[f"AveRaw_O2_{cm}"] * 100
                elif f"AvgRawO2_{cm}" in df:
                    df[f"AveRaw_O2_{cm}_x100"] = df[f"AvgRawO2_{cm}"] * 100

                if f"VWCdielectric_{cm}_Avg" in df:
                    df[f"VWCdielectric_{cm}_Avg_x100"] = df[f"VWCdielectric_{cm}_Avg"] * 100
                elif f"VWCdielectric_{cm}" in df:
                    df[f"VWCdielectric_{cm}_Avg_x100"] = df[f"VWCdielectric_{cm}"] * 100

                if f"VWCmineral_{cm}_Avg" in df:
                    df[f"VWCmineral_{cm}_Avg_x10000"] = df[f"VWCmineral_{cm}_Avg"] * 10000
                elif f"VWCmineral_{cm}" in df:
                    df[f"VWCmineral_{cm}_Avg_x10000"] = df[f"VWCmineral_{cm}"] * 10000

                if f"electricalConductivity_{cm}_Avg" in df:
                    df[f"electricalConductivity_{cm}_Avg_x10"] = df[f"electricalConductivity_{cm}_Avg"] * 10
                elif f"electricalConductivity_{cm}" in df:
                    df[f"electricalConductivity_{cm}_Avg_x10"] = df[f"electricalConductivity_{cm}"] * 10
            try:
                generate_il_plots(fig, df)
            except:
                continue

        outfile = "plots/%s" % (f['filename'].replace(".xlsx", ".pdf"))
        plt.savefig(outfile, format="pdf", bbox_inches="tight")
        print("Generated "+outfile)
        upload_results(src, outfile)

print("Done.")
