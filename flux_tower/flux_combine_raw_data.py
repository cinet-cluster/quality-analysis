import requests
import pickle
import io
import ssl
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from urllib.request import urlopen
from datetime import datetime as dt


MY_API_KEY = ''  # Replace with valid Clowder API key
clowder_base_uri = "https://cinet.ncsa.illinois.edu/clowder/api"  # CINet

clowder_repository = '62a8a3e4e4b01bc1aa9a3d3c'    # ID of Clowder dataset repository where files will be uploaded
clowder_repository = '6489d855e4b0bcb1fe243179'    # MAX'S DATASET

dataset_id_flux25new = "61686d62e4b00ca690e54657"  # flux tower agg dataset (updated daily w/new file)
dataset_id_flux10old = "61686da4e4b00ca690e5466e"  # old data (up to Dec 2021) for 10m height
dataset_id_flux10new = "6218f72ae4b024d4c1881561"  # should have up to current data for 10m height

base_headers = {'X-API-key':MY_API_KEY}
headers = {**base_headers, 'Content-type': 'application/json',
           'accept': 'application/json'}

# Clowder utility functions
def list_files_of_dataset(dataset_id):
    r = requests.get(clowder_base_uri + '/datasets/' + dataset_id + '/files',
                     headers=headers, verify=False)
    if r.status_code == 200:
        return r.json()
    else:
        return r.text

def list_tags_of_file(file_id):
    r = requests.get(clowder_base_uri + '/files/' + file_id + '/tags', headers=headers, verify=False)
    if r.status_code == 200:
        return r.json()
    else:
        return r.text

def upload_files_to_dataset(dataset_id, filenames):
    files = [('File', open(fname, 'rb')) for fname in filenames]
    r = requests.post(clowder_base_uri + '/datasets/' + dataset_id + '/files',
                      files=files, headers=base_headers, verify=False)
    if r.status_code == 200:
        return r.json()
    else:
        return r.text

def send_email_alert(message, attachments=None):
    to_emails = ["cinet-pipelines@lists.illinois.edu"]
    from_email = "cinet-flux-monitor@ncsa.illinois.edu"
    smtp_server = "smtp.ncsa.uiuc.edu"
    port = 25  # For starttls
    context = ssl.create_default_context()

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = ",".join(to_emails)
    msg['Subject'] = "CINet Flux Tower Report"
    msg.attach(MIMEText(message))

    # After the file is closed
    for attachment in attachments:  # add files to the message
        with open(attachment, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(attachment)
            )
        part.add_header('Content-Disposition', 'attachment', filename=attachment)
        msg.attach(part)

    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls(context=context)  # Secure the connection
        server.sendmail(from_email, ",".join(to_emails), msg.as_string())
        server.close()
    except Exception as e:
        print("Error sending email: %s" % e)


# 25m DATA -------------------------------------------------------------------
files_flux25 = list_files_of_dataset(dataset_id_flux25new)

newdata = []
# download file bytes
for file in files_flux25:
    if file['filename'].startswith("Flux Tower Data ("):
        url = requests.get(clowder_base_uri + '/files/' + file['id'] + '/blob?key=' + MY_API_KEY, verify=False)
        newdata.append(url.content)
        break
newdata_vars = pd.read_csv(io.StringIO(newdata[0].decode('utf-8')), header=1, skiprows=[2, 3]).columns.tolist()

# download list from excel file...
file_list = pd.read_csv('filenames_in_order.csv')

# also download a list of column names we are looking for...
var_list = pd.read_csv('variable_names.csv')
var_names = list(var_list.variables)

# determine which variables are present
vars_all = []
common_vars = []
missing_vars = []
i = 0
for ind, f in enumerate(file_list.filenames):
    head = file_list.header_line[ind]
    if f == 'RECENT':  # last in list, append newest data directly from Clowder repository
        var_file = newdata_vars
    else:
        if head == 1:
            var_file = pd.read_csv('data_directfromClowder/' + f, header=head - 1, skiprows=[1, 2]).columns.tolist()
        else:
            var_file = pd.read_csv('data_directfromClowder/' + f, header=1, skiprows=[2, 3]).columns.tolist()
    # find the subset of variables that file has, out of the full possible list of variable names
    intersect = list(set(var_file) & set(var_names))
    nonintersect = list(set(var_file) ^ set(var_names))
    missing_vars.append(nonintersect)  # different set of missing vars in each

# compile data into one big Dataframe of lists...with same variables
data_all = [
    pd.read_csv(io.StringIO(newdata[0].decode('utf-8')), header=1, skiprows=[2, 3], usecols=intersect)
]

# get date in a datetime format (NewDate index)...and concatenate all dataframes into one big one: DATA_all
for ct, data in enumerate(data_all):
    data['NewDate'] = pd.to_datetime(data['TIMESTAMP'], infer_datetime_format=True) + pd.DateOffset(hours=1)
    if ct == 0:
        DATA_all = data
    else:
        DATA_all = pd.concat([DATA_all, data], ignore_index=True)

# get rid of any colummns that start with "Unnamed"
for c in DATA_all.columns:
    if 'Unnamed' in c:
        DATA_all = DATA_all.drop(labels=c, axis="columns")

# duplicates are removed, gaps are filled with NAN values, data are re-ordered in time
deltas = DATA_all['NewDate'].diff()[1:]
gaps = deltas[deltas > timedelta(minutes=15)]
dups = deltas[deltas < timedelta(minutes=15)]

gap_start = []
for i, g in gaps.iteritems():
    gap_start.append(DATA_all['NewDate'][i-1])
d = {'Gaps': gaps, 'Start': gap_start}
df_gaps = pd.DataFrame(data=d)

dup_start = []
for i, g in dups.iteritems():
    dup_start.append(DATA_all['NewDate'][i-1])
d = {'Duplicates': dups, 'Start': dup_start}
df_dups = pd.DataFrame(data=d)

# plot gaps and duplicates in hours (converted from nanoseconds)
try:
    plt.figure(1)
    plt.figure(figsize=(5, 3))
    plt.plot(DATA_all['NewDate'][0:-1], np.asfarray(deltas)/(10**9 * 3600))
    plt.plot(df_dups.Start, df_dups.Duplicates/(10**9 * 3600), '.r')
    plt.plot(df_gaps.Start, df_gaps.Gaps/(10**9 * 3600), '.b')
    plt.title('1: Gaps/Duplicates, 25m Flux data (direct from Clowder)')
    plt.legend(['delta (time step)', 'duplicates', 'gaps'])
    plt.ylim([-500, 1000])
    plt.ylabel('Hours (pos = gap, neg = duplicate)')
except Exception as e:
    print("ERR: 25m - Fig 1 - %s" % e)

# delete duplicates of data...and SORT since random values out of line
DATA_nodups = DATA_all.drop_duplicates(subset='NewDate', keep='first')
DATA_sorted = DATA_nodups.sort_values(by=['NewDate'])

try:
    plt.figure(2)
    plt.figure(figsize=(5, 2))
    deltas = DATA_sorted['NewDate'].diff()[1:]
    plt.semilogy(DATA_sorted['NewDate'][0:-1], np.asfarray(deltas)/(10**9 * 3600))
    plt.title('2: Gaps/Duplicates, 25m Flux data (sorted, remove duplicates)')
    plt.ylabel('Hours (pos = gap)')
except Exception as e:
    print("ERR: 25m - Fig 2 - %s" % e)

# fill gaps within DATA_new...this will insert NaNs for all gap values
DATA25_rawfinal = DATA_sorted.set_index('NewDate', drop=True)
# Remove the last row if the timestamp index is NaT
if pd.isnull(DATA25_rawfinal.index[-1]):
    DATA25_rawfinal = DATA25_rawfinal.drop(DATA25_rawfinal.index[-1])
DATA25_rawfinal = DATA25_rawfinal.asfreq('15min').reset_index().append(DATA_sorted.iloc[-1]).reset_index(drop=True)

# convert columns to numeric for future data analysis...
for i, colname in enumerate(DATA25_rawfinal):
    if colname != 'NewDate' and colname != 'TIMESTAMP':
        DATA25_rawfinal[colname] = pd.to_numeric(DATA25_rawfinal[colname], errors='coerce')
# remove very last row from dataframe (for some reason the timestamp repeats)
DATA25_rawfinal.drop(DATA25_rawfinal.tail(1).index, inplace=True)

pickle_filename = "FluxData_Raw_ALL.pickle"
# delete existing pickle file from destination dataset
#files = list_files_of_dataset(clowder_repository)
#for file in files:
#    if file['filename'] == pickle_filename:
#        requests.delete(clowder_base_uri + '/files/' + file['id'], headers=base_headers, verify=False)
# dump & upload new pickle file
#pickle.dump(DATA25_rawfinal, open(pickle_filename, "wb"))
# upload_files_to_dataset(clowder_repository, [pickle_filename])
#DATA25_rawfinal.to_csv(pickle_filename.replace(".pickle", ".csv"))


# 10m DATA -------------------------------------------------------------------
files_flux10old = list_files_of_dataset(dataset_id_flux10old)
files_flux10new = list_files_of_dataset(dataset_id_flux10new)
files = list_files_of_dataset(clowder_repository)

# load existing pickle file
# try:
#     for file in files:
#         if file['filename'] == flux10_pickle:
#             url = clowder_base_uri + '/files/' + file['id'] + '/blob?key=' + MY_API_KEY
#             gcontext = ssl.SSLContext()  # Only for gangstars
#             data_10m_all = pickle.load(urlopen(url, context=gcontext))
#             break
# except Exception as e:
# the slow way (only run if the big pickle files gets corrupted for some reason)
flag = 0
for file in files_flux10old:
    # load file as a dataframe
    url = requests.get(clowder_base_uri + '/files/' + file['id'] + '/blob?key=' + MY_API_KEY, verify=False)
    data_file = pd.read_csv(io.StringIO(url.content.decode('utf-8')),sep='\t')
    data_file = data_file.drop(axis=0, index=0)

    # update on 8/14/2023 AEG: according to Steve (and solar radiation analysis) - time zone is one hour behind CST
    # so I'm altering the 'NewDate' timestamp to be one hour ahead (putting into CST)
    vect = data_file.date.astype(str) + ' ' + data_file.time.astype(str)
    datevect = pd.to_datetime(vect, infer_datetime_format=True)
    data_file['NewDate'] = datevect + pd.DateOffset(hours=1)

    if flag == 0:
        data_10m_all = data_file
    else:
        data_10m_all = pd.concat([data_10m_all, data_file])
    flag = 1
data_10m_all = data_10m_all.sort_values(by=['NewDate'])
last_time = data_10m_all['NewDate'].iloc[-1]

for file in files_flux10new:
    file_date = file['filename'][0:10]
    datetime_object = datetime.strptime(file_date, '%Y-%m-%d').date()

    if datetime_object > last_time:
        # load file as a dataframe
        url = requests.get(clowder_base_uri + '/files/' + file['id'] + '/blob?key=' + MY_API_KEY, verify=False)
        data_file = pd.read_csv(io.StringIO(url.content.decode('utf-8')), sep='\t')
        data_file = data_file.drop(axis=0, index=0)

        vect = data_file.date.astype(str) + ' ' + data_file.time.astype(str)
        datevect = pd.to_datetime(vect, infer_datetime_format=True)
        data_file['NewDate'] = datevect + pd.DateOffset(hours=1)  # get time zone to central (an hour ahead)
        data_10m_all = pd.concat([data_10m_all, data_file])

# force all variables (except time stamp, filename) to be numeric (some appear as object dtypes)
for i, colname in enumerate(data_10m_all):
    if colname not in ['NewDate', 'filename', 'date']:
        data_10m_all[colname] = pd.to_numeric(data_10m_all[colname], errors='coerce')

deltas = data_10m_all['NewDate'].diff()[1:]
gaps = deltas[deltas > timedelta(minutes=30.5)]
dups = deltas[deltas < timedelta(minutes=29.5)]

gap_start = []
dup_start = []

try:
    plt.figure(1)
    plt.figure(figsize=(5, 2))
    # plot gaps and duplicates in hours (converted from nanoseconds)
    plt.plot(data_10m_all['NewDate'][0:-1], np.asfarray(deltas) / (10 ** 9 * 3600))
    plt.ylabel('Hours (pos = gap, neg = duplicate)')
    plt.title('Gaps and Duplicates for 10m flux tower instrument')
except Exception as e:
    print("ERR: 10m - Fig 1 - %s" % e)

# delete duplicates of data...and SORT since random values out of line
DATA_nodups = data_10m_all.drop_duplicates(subset='NewDate', keep='first')
deltas = DATA_nodups['NewDate'].diff()[1:]

# fill gaps within DATA_new...this will insert NaNs for all gap values...
DATA10_rawfinal = DATA_nodups.set_index('NewDate', drop=True)
DATA10_rawfinal = DATA10_rawfinal.asfreq('30min').reset_index().append(DATA_nodups.iloc[-1]).reset_index(drop=True)
deltas = DATA10_rawfinal['NewDate'].diff()[1:]

try:
    plt.figure(2)
    plt.plot(DATA10_rawfinal['NewDate'][0:-1], np.asfarray(deltas) / (10 ** 9 * 3600))
    plt.title('Gaps/Duplicates, 10m Flux data (dates correctly sorted, gaps NAN filled)')
    plt.ylabel('Hours (pos = gap, neg = duplicate)')
except Exception as e:
    print("ERR: 10m - Fig 2 - %s" % e)

# drop last value (for some reason is a duplicate value)
DATA10_rawfinal.drop(DATA10_rawfinal.tail(1).index, inplace=True)

flux10_pickle = "FluxData_Raw_10m.pickle"
# delete existing pickle file
#files = list_files_of_dataset(clowder_repository)
#for file in files:
#    if file['filename'] == flux10_pickle:
#        requests.delete(clowder_base_uri + '/files/' + file['id'], headers=base_headers, verify=False)
# dump & upload new pickle file
#pickle.dump(DATA10_rawfinal, open(flux10_pickle, "wb"))
#upload_files_to_dataset(clowder_repository, [flux10_pickle])
#DATA10_rawfinal.to_csv(flux10_pickle.replace(".pickle", ".csv"))


# WEEKLY PLOT DATA PDF -------------------------------------------------------------------
N = 30  # number of days to plot
DATA25_rawfinal['Hour']=pd.DatetimeIndex(DATA25_rawfinal['NewDate']).hour
DATA10_rawfinal['Hour']=pd.DatetimeIndex(DATA10_rawfinal['NewDate']).hour
date_val = dt.now() - timedelta(days=N)
current25 = DATA25_rawfinal[DATA25_rawfinal['NewDate'] > date_val]
current10 = DATA10_rawfinal[DATA10_rawfinal['NewDate'] > date_val]
x_lim_vals = [date_val, dt.now()]

npts25 = 96*N
npts10 = 48*N

plt.figure(figsize=(10, 15))
plt.tight_layout()

try:
    plt.subplot(721)
    plt.plot(current25['NewDate'], np.asfarray(current25['Rn_Avg']), 'b')
    plt.title('Net radiation')
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 721 - %s" % e)

try:
    plt.subplot(722)
    plt.plot(current25['NewDate'], np.asfarray(current25['Precip_Tot']))
    plt.plot(current25['NewDate'], np.cumsum(np.asfarray(current25['Precip_Tot'])))
    plt.title('PPT')
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 722 - %s" % e)

try:
    plt.subplot(723)
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_VWC_5cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_VWC_15cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_VWC_30cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_VWC_50cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_VWC_100cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_VWC_200cm_Avg']))
    plt.legend(['5cm', '15cm', '30cm', '50cm', '100cm', '200cm'])
    plt.title('Soil moisture')
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 723 - %s" % e)

try:
    plt.subplot(724)
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_T_5cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_T_15cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_T_30cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_T_50cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_T_100cm_Avg']))
    plt.plot(current25['NewDate'], np.asfarray(current25['D5TE_T_200cm_Avg']))
    plt.title('Soil temp')
    plt.legend(['5cm', '15cm', '30cm', '50cm', '100cm', '200cm'])
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 724 - %s" % e)

try:
    plt.subplot(725)
    plt.plot(current25['NewDate'], np.asfarray(current25['NDVI_Avg']))
    plt.title('NDVI')
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 725 - %s" % e)

try:
    plt.subplot(726)
    plt.plot(current25['NewDate'], np.asfarray(current25['LE_li_wpl']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(current10['LE']), 'r')
    plt.ylim(-200, 600)
    plt.title('LE')
    plt.xlim(x_lim_vals)
    plt.legend(['25m', '10m'])
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 726 - %s" % e)

try:
    plt.subplot(727)
    plt.plot(current25['NewDate'], np.asfarray(current25['Hc_li']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(current10['H']), 'r')
    plt.ylim(-200, 500)
    plt.title('H')
    plt.legend(['25m H', '10m H'])
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 727 - %s" % e)

try:
    plt.subplot(728)
    plt.plot(current25['NewDate'], np.asfarray(current25['shf_Avg(1)']), 'g')
    plt.plot(current25['NewDate'], np.asfarray(current25['shf_Avg(2)']), 'k')
    plt.title('G')
    plt.legend('G1', 'G2')
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 728 - %s" % e)

try:
    plt.subplot(729)
    plt.plot(current25['NewDate'], np.asfarray(current25['tau']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(-current10['Tau']), 'r')
    plt.title('tau')
    plt.legend('25m', '10m')
    plt.ylim([-1, 5])
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 729 - %s" % e)

try:
    plt.subplot(7, 2, 10)
    plt.plot(current25['NewDate'], np.asfarray(current25['T_tmpr_rh_mean']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(current10['air_temperature']) - 273, 'r')
    plt.title('Ta')
    plt.legend(['25m', '10m'])
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 7210 - %s" % e)

try:
    plt.subplot(7, 2, 11)
    plt.plot(current25['NewDate'], np.asfarray(current25['rslt_wnd_spd']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(current10['wind_speed']), 'r')
    plt.title('wind speed')
    plt.legend(['25m', '10m'])
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 7211 - %s" % e)

try:
    plt.subplot(7, 2, 12)
    plt.plot(current25['NewDate'], np.asfarray(current25['RH_tmpr_rh_mean']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(current10['RH']), 'r')
    plt.title('RH')
    plt.legend(['25m', '10m'])
    plt.xlim(x_lim_vals)
    plt.xticks([])
except Exception as e:
    print("ERR: Data - Fig 7212 - %s" % e)

try:
    plt.subplot(7, 2, 13)
    plt.plot(current25['NewDate'], np.asfarray(current25['wnd_dir_compass']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(current10['wind_dir']), 'r')
    plt.title('wind dir')
    plt.legend(['25m', '10m'])
    plt.xlim(x_lim_vals)
    plt.xticks(rotation=70)
except Exception as e:
    print("ERR: Data - Fig 7213 - %s" % e)

try:
    plt.subplot(7, 2, 14)
    plt.plot(current25['NewDate'], np.asfarray(current25['Fc_li_wpl']), 'b')
    plt.plot(current10['NewDate'], np.asfarray(current10['co2_flux'] * (12.01 + 16 * 2) / 1000), 'r')
    plt.title('Fc')
    plt.legend(['25m', '10m'])
    plt.xlim(x_lim_vals)
    plt.ylim([-1, 1])
    plt.xticks(rotation=70)
except Exception as e:
    print("ERR: Data - Fig 7214 - %s" % e)

plt.savefig("weekly_plot_data.pdf", format="pdf", bbox_inches="tight")


# WEEKLY PLOT DIURNAL PDF -------------------------------------------------------------------
mean_vals25 = []
std_vals25 = []
mean_vals10 = []
std_vals10 = []

for h in range(24):
    rslt_df25 = current25[current25['Hour'] == h]
    rslt_df10 = current10[current10['Hour'] == h]

    mean_h = rslt_df25.mean()
    stdev_h = rslt_df25.std()
    mean_vals25.append(mean_h)
    std_vals25.append(stdev_h)

    mean_h = rslt_df10.mean()
    stdev_h = rslt_df10.std()
    mean_vals10.append(mean_h)
    std_vals10.append(stdev_h)

df_means25 = pd.concat(mean_vals25, axis=1)
df_stdevs25 = pd.concat(std_vals25, axis=1)
df_means10 = pd.concat(mean_vals10, axis=1)
df_stdevs10 = pd.concat(std_vals10, axis=1)
df_means25 = df_means25.transpose()
df_means10 = df_means10.transpose()

plt.figure(figsize=(10,15))
plt.tight_layout()

plt.subplot(721)
plt.plot(np.asfarray(df_means25['Rn_Avg']), 'b')
plt.title('Net radiation')
plt.xticks([])

plt.subplot(722)
plt.plot(np.asfarray(df_means25['Precip_Tot']))
plt.title('PPT')
plt.xticks([])

plt.subplot(723)
plt.plot(np.asfarray(df_means25['D5TE_VWC_5cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_VWC_15cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_VWC_30cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_VWC_50cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_VWC_100cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_VWC_200cm_Avg']))
plt.legend(['5cm', '15cm', '30cm', '50cm', '100cm', '200cm'])
plt.title('Soil moisture')
plt.xticks([])

plt.subplot(724)
plt.plot(np.asfarray(df_means25['D5TE_T_5cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_T_15cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_T_30cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_T_50cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_T_100cm_Avg']))
plt.plot(np.asfarray(df_means25['D5TE_T_200cm_Avg']))
plt.title('Soil temp')
plt.legend(['5cm', '15cm', '30cm', '50cm', '100cm', '200cm'])
plt.xticks([])

plt.subplot(725)
plt.plot(np.asfarray(df_means25['NDVI_Avg']))
plt.title('NDVI')
plt.xticks([])

plt.subplot(726)
plt.plot(np.asfarray(df_means25['LE_li_wpl']), 'b')
plt.plot(np.asfarray(df_means10['LE']), 'r')
plt.ylim(-200, 500)
plt.title('LE')
plt.legend(['25m', '10m'])
plt.xticks([])

plt.subplot(727)
plt.plot(np.asfarray(df_means25['Hc_li']), 'b')
plt.plot(np.asfarray(df_means10['H']), 'r')
plt.ylim(-200, 500)
plt.title('H')
plt.legend(['25m H', '10m H'])
plt.xticks([])

plt.subplot(728)
plt.plot(np.asfarray(df_means25['shf_Avg(1)']), 'g')
plt.plot(np.asfarray(df_means25['shf_Avg(2)']), 'k')
plt.title('G')
plt.legend('G1', 'G2')
plt.xticks([])

plt.subplot(729)
plt.plot(np.asfarray(df_means25['tau']), 'b')
plt.plot(np.asfarray(-df_means10['Tau']), 'r')
plt.title('tau')
plt.legend('25m', '10m')
plt.xticks([])


plt.subplot(7, 2, 10)
plt.plot(np.asfarray(df_means25['T_tmpr_rh_mean']), 'b')
plt.plot(np.asfarray(df_means10['air_temperature'])-273, 'r')
plt.title('Ta')
plt.legend(['25m', '10m'])
plt.xticks([])

plt.subplot(7, 2, 11)
plt.plot(np.asfarray(df_means25['rslt_wnd_spd']), 'b')
plt.plot(np.asfarray(df_means10['wind_speed']), 'r')
plt.title('wind speed')
plt.legend(['25m', '10m'])
plt.xticks([])

plt.subplot(7, 2, 12)
plt.plot(np.asfarray(df_means25['RH_tmpr_rh_mean']), 'b')
plt.plot(np.asfarray(df_means10['RH']), 'r')
plt.title('RH')
plt.legend(['25m', '10m'])
plt.xticks([])

plt.subplot(7, 2, 13)
plt.plot(np.asfarray(df_means25['wnd_dir_compass']), 'b')
plt.plot(np.asfarray(df_means10['wind_dir']), 'r')
plt.title('wind dir')
plt.legend(['25m', '10m'])
plt.xticks(rotation=70)

plt.subplot(7, 2, 14)
plt.plot(np.asfarray(df_means25['Fc_li_wpl']), 'b')
plt.plot(np.asfarray(df_means10['co2_flux']*(12.01+16*2)/1000), 'r')
plt.title('Fc')
plt.legend(['25m', '10m'])
plt.xticks(rotation=70)

plt.savefig("weekly_plot_diurnal.pdf", format="pdf", bbox_inches="tight")


# WEEKLY PLOT DIAGNOSTIC PDF -------------------------------------------------------------------
diagvars_25m = ['sonic_samples_Tot', 'irga_li_samples_Tot', 'slowsequence_1_Tot', 'Panel_Tmpr_Avg']
diagvars_10m = ['qc_LE', 'qc_H', 'qc_co2_flux', 'co2_signal_strength_7500_mean']

# quality flags for fluxes: 0 is good, 1 is ok, 2 is "don't use"
plt.figure(figsize=(10, 10))
plt.tight_layout()

def add_diag_subplot(plt, num, x, y, title, plot_args=None, rot_x=False):
    """Helper function that includes try/catch in case time series have unexpected values."""
    try:
        plt.subplot(num)
        if plot_args is None:
            plt.plot(x, y)
        else:
            plt.plot(x, y, plot_args)
        plt.title(title)
        plt.xlim(x_lim_vals)
        if rot_x:
            plt.xticks(rotation=70)
        else:
            plt.xticks([])
    except Exception as e:
        print("ERR: Diag - Fig %s - %s" % (num, e))


add_diag_subplot(plt, 421,
            current25['NewDate'],
            np.asfarray(current25['Precip_Tot']),
            'Precip')

add_diag_subplot(plt, 422,
            current25['NewDate'],
            np.asfarray(current25['T_tmpr_rh_mean']),
            'Air temp(25m)')

add_diag_subplot(plt, 423,
            current25['NewDate'],
            np.asfarray(current25['sonic_samples_Tot']),
            '25m: sonic samples')

add_diag_subplot(plt, 424,
            current25['NewDate'],
            np.asfarray(current25['irga_li_samples_Tot']),
            '25m: licor samples')

add_diag_subplot(plt, 425,
            current10['NewDate'],
            np.asfarray(current10['qc_LE']),
            '10m: LE quality flag', plot_args='.')

add_diag_subplot(plt, 426,
            current10['NewDate'],
            np.asfarray(current10['qc_H']),
            '10m: H quality flag', plot_args='.')

add_diag_subplot(plt, 427,
            current10['NewDate'],
            np.asfarray(current10['qc_co2_flux']),
            '10m: Fc quality flag', plot_args='.', rot_x=True)

add_diag_subplot(plt, 428,
            current10['NewDate'],
            np.asfarray(current10['co2_signal_strength_7500_mean']),
            '10m: CO2 signal strength', rot_x=True)

plt.savefig("weekly_plot_diag.pdf", format="pdf", bbox_inches="tight")


# WEEKLY FLUX DIGEST -------------------------------------------------------------------
N = 20  # number of days of history to consider (for seasonally changing variables)
npts25 = 96 * N
npts10 = 48 * N
data_25m_history = DATA25_rawfinal
data_10m_history = DATA10_rawfinal
data_25m_1day = DATA25_rawfinal[-96:]
data_10m_1day = DATA10_rawfinal[-48:]
frac25_day = data_25m_1day.count() / 96  # fraction of non-nan values
frac10_day = data_10m_1day.count() / 48  # fraction of non-nan values

# make sure file is updating: check for last timestamp to be within 1 day of today
diff25 = dt.now() - data_25m_1day.NewDate[-1:]
diff10 = dt.now()-data_10m_1day.NewDate[-1:]
diff25 = float(diff25 / np.timedelta64(1, 'D'))
diff10 = float(diff10 / np.timedelta64(1, 'D'))

recent_flag25 = 0
recent_flag10 = 0
if diff25 > 1:
    recent_flag25 = 1
if diff10 > 1:
    recent_flag10 = 1

# flag variables if over half nan values
check_var_table = pd.read_csv('tower_check_variables.csv')
nan_flags25 = []
for i, d in enumerate(check_var_table['25m']):
    if d in frac25_day and frac25_day[d] < 0.5:
        nan_flags25.append(d)
    elif d not in frac25_day:
        nan_flags25.append(d)
    # flag for spikes: values over/under 3*std or 99.5th percentile, whichever is larger
nan_flags10 = []
for i, d in enumerate(check_var_table['10m']):
    if d == 'x':
        break
    if frac10_day[d] < 0.5:
        nan_flags10.append(d)

try:
    plt.figure(figsize=(7, 1))
    plt.subplot(141)
    plt.hist(data_10m_1day.qc_LE)
    plt.title('LE flags')
except Exception as e:
    print("ERR: Flags - Fig 141 - %s" % e)

try:
    plt.subplot(142)
    plt.hist(data_10m_1day.qc_H)
    plt.title('H flags')
except Exception as e:
    print("ERR: Flags - Fig 142 - %s" % e)

try:
    plt.subplot(143)
    plt.hist(data_10m_1day.qc_co2_flux)
    plt.title('Fc flags')
except Exception as e:
    print("ERR: Flags - Fig 143 - %s" % e)

try:
    plt.subplot(144)
    plt.hist(data_10m_1day.qc_Tau)
    plt.title('Tau flags')
except Exception as e:
    print("ERR: Flags - Fig 144 - %s" % e)

try:
    # checking on missing grount heat flux plate...
    plt.plot(DATA25_rawfinal['NewDate'][-100000:], DATA25_rawfinal['shf_Avg(2)'][-100000:])
    plt.plot(DATA25_rawfinal['NewDate'][-100000:], DATA25_rawfinal['shf_Avg(1)'][-100000:])
    plt.ylim([-50, 50])
except Exception as e:
    print("ERR: Flags - %s" % e)

plt.savefig("weekly_flux_digest.pdf", format="pdf", bbox_inches="tight")


# FINISH AND UPLOAD -------------------------------------------------------------------
summary = f"""
    The following variables have >50% nan values:\n
    main tower: {nan_flags25}\n
    10m height Licor: {nan_flags10}\n
    \n
    Data is updated since (days ago):\n
    25m: {diff25}\n
    10m: {diff10}\n
    \n
    Precip in past N days (mm and inches):\n
    mm: {np.nansum(current25['Precip_Tot'])}\n
    in: {np.nansum(current25['Precip_Tot'])*0.03937}\n
"""

# delete existing PDF uploads & upload
target_pdfs = ["weekly_plot_data.pdf", "weekly_plot_diurnal.pdf", "weekly_plot_diag.pdf"]
files = list_files_of_dataset(clowder_repository)
for file in files:
    if file['filename'] in target_pdfs:
        requests.delete(clowder_base_uri + '/files/' + file['id'], headers=base_headers, verify=False)
upload_files_to_dataset(clowder_repository, target_pdfs)
send_email_alert(summary, target_pdfs)
