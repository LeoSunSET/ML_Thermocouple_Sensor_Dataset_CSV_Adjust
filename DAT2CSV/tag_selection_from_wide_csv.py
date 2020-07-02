import pandas
import numpy as np
from os import listdir
from os.path import isfile, join


# Initialization
root_data_csv_path = "D:\RHF_DS\\raw_data\\"
cleaned_data_csv_path = "D:\RHF_DS\\clean_data\\Clean_202001.csv"
timestamp_col = ["time"]
all_tag_names_no_ts = ["COMBUSTION AIR PRESSURE", "FURNACE PRESSURE", "COMBUSTION AIR TEMP", "ZONE 6 EAST THERMOCOUPLE 010601", "ZONE 6 CENTRAL THERMOCOUPLE 010602", "ZONE 6 WEST THERMOCOUPLE 010603", "ZONE 5 EAST THERMOCOUPLE 010501", "ZONE 5 CENTRAL THERMOCOUPLE 010502", "ZONE 5 WEST THERMOCOUPLE 010503", "ZONE 4 NORTH EAST THERMOCOUPLE 010401", "ZONE 4 NORTH WEST THERMOCOUPLE 010402", "ZONE 4 CENTER EAST THERMOCOUPLE 010403", "ZONE 4 CENTER WEST THERMOCOUPLE 010404", "ZONE 4 SOUTH EAST THERMOCOUPLE 010405", "ZONE 4 SOUTH WEST THERMOCOUPLE 010406", "ZONE 3 NORTH EAST THERMOCOUPLE 010301", "ZONE 3 NORTH WEST THERMOCOUPLE 010302", "ZONE 3 CENTER EAST THERMOCOUPLE 010303", "ZONE 3 CENTER WEST THERMOCOUPLE 010304", "ZONE 3 SOUTH EAST THERMOCOUPLE 010305", "ZONE 3 SOUTH WEST THERMOCOUPLE 010306", "ZONE 2 NORTH EAST THERMOCOUPLE 010201", "ZONE 2 NORTH WEST THERMOCOUPLE 010202", "ZONE 2 SOUTH EAST THERMOCOUPLE 010203", "ZONE 2 SOUTH WEST THERMOCOUPLE 010204", "ZONE 1 NORTH EAST THERMOCOUPLE 010101", "ZONE 1 NORTH WEST THERMOCOUPLE 010102", "ZONE 1 SOUTH EAST THERMOCOUPLE 010103", "ZONE 1 SOUTH WEST THERMOCOUPLE 010104", "PASSIVE ZONE NORTH WEST THERMOCOUPLE 10001", "PASSIVE ZONE NORTH EAST THERMOCOUPLE 10002", "PASSIVE ZONE SOUTH WEST THERMOCOUPLE 10003", "PASSIVE ZONE SOUTH EAST THERMOCOUPLE 10004", "ZONE 6 AIR/FUEL RATIO", "ZONE 5 AIR/FUEL RATIO", "ZONE 3/4 AIR/FUEL RATIO", "ZONE 1/2 AIR/FUEL RATIO", "ZONE 6 GAS FLOW", "ZONE 6 AIR FLOW", "ZONE 5 GAS FLOW", "ZONE 5 AIR FLOW", "ZONE 3/4 GAS FLOW", "ZONE 3/4 AIR FLOW", "ZONE 1/2 GAS FLOW", "ZONE 1/2 AIR FLOW", "TEMP BEFORE RECUPERATOR 30101", "TEMP AFTER RECUPERATOR 30206", "GAS TEMP 62201", "RHF BEAMS INLET WATER FLOW", "RHF BEAMS OUTLET WATER FLOW", "RHF BEAMS INLET WATER PRESS", "RHF KICK IN HOLES WATER FLOW", "RHF KICK IN FINGERS WATER FLOW", "RHF CHARGE ROLLS WATER FLOW", "RHF KICK OFF HOLES WATER FLOW", "RHF KICK OFF FINGERS WATER FLOW", "RHF DISCHARGE ROLLS WATER FLOW", "ZONE 6 GAS PV", "ZONE 5 GAS PV", "ZONE 3 GAS PV", "ZONE 1 GAS PV", "ZONE 6 AIR PV", "ZONE 5 AIR PV", "ZONE 3 AIR PV", "ZONE 1 AIR PV"]
all_tag_names_no_ts_clean = []
all_data_csv_path = []
pdf_returned_last = None
pdf_returned = None

# Get all files
onlyfiles = [f for f in listdir(root_data_csv_path) if isfile(join(root_data_csv_path, f))]

# Get list of the files path
for files in onlyfiles:
    all_data_csv_path.append(root_data_csv_path + files)

# Change all tag_names
def clean_tag_name (tag_names):
    tag_names_clean = []
    for c_name in tag_names:
    tag_name_tmp = c_name.replace(" ", "_")
    tag_name_tmp = tag_name_tmp.replace("(", "_")
    tag_name_tmp = tag_name_tmp.replace(")", "")
    tag_name_tmp = tag_name_tmp.replace("/", "_")
    tag_name_tmp = tag_name_tmp.replace("\\", "_")
    tag_names_clean.append(tag_name_tmp)

    return tag_names_clean

all_tag_names_no_ts_clean = clean_tag_name(all_tag_names_no_ts)

print("##############################################")
print("#########      START EXPORTING     ##########")
print("##############################################")

# Get all data and concatenate it
for data_csv_path, data_csv_path_index in zip(all_data_csv_path, range(len(all_data_csv_path))):
    # File Initialization
    tag_names_no_ts = []
    tag_names_no_ts_clean = []

    print("[INFO] Processing file: %s" % onlyfiles[data_csv_path_index])

    # Load Data
    pdf = pandas.read_csv(data_csv_path, delimiter='\t', header=1)

    # Get column names
    all_columns_names = list(pdf.columns.values)

    # Get column names without timestamp
    for c_name in all_columns_names:
        if c_name != timestamp_col[0]:
            tag_names_no_ts.append(c_name)

    # Get timestamp data
    ts = pdf[timestamp_col].values
    ts_r, ts_c = ts.shape

    # Get data without timestamp
    data = pdf[tag_names_no_ts].values

    # Replace timestamp data "." by "/" only on the 2 first ".". Replace "000" by "" to convert timestamp to millisecond.
    # Example of input timestamp str: 01.01.2019 00:11:10.250000
    # Example of output timestamp str: 01/01/2019 00:11:10.250
    for ts_row_index in range(ts_r):
        ts_tmp = ts[ts_row_index, 0]
        ts_tmp = ts_tmp.replace(".", "/", 2)
        ts_tmp = ts_tmp.replace("000", "", 1)
        ts[ts_row_index, 0] = ts_tmp

    # Change current file tag_names
    tag_names_no_ts_clean = clean_tag_name(tag_names_no_ts)

    # Create empty matrix
    all_data_per_file = np.empty([ts_r, len(all_tag_names_no_ts)])

    # Create all data matrix
    for all_tag_name_index in range(len(all_tag_names_no_ts_clean)):
        for file_tag_name_index in range(len(tag_names_no_ts_clean)):
            if tag_names_no_ts_clean[file_tag_name_index] == all_tag_names_no_ts_clean[all_tag_name_index]:
                all_data_per_file[:, all_tag_name_index] = data[:, file_tag_name_index]

    # Create Pandas Data Frame with Timestamps
    pdf_ts = pandas.DataFrame(data=ts,  # values
                              columns=timestamp_col)  # 1st row as the column names

    # Create Pandas Data Frame with numpy_array
    pdf_data = pandas.DataFrame(data=all_data_per_file,  # values
                                columns=all_tag_names_no_ts_clean)  # 1st row as the column names

    # Concatenate Columns, the result Data Frame contains Timestamp + numpy_array
    pdf_returned = pandas.concat([pdf_ts, pdf_data], axis=1)

    # Concatenate the last and current data-frame if it not the first file
    if data_csv_path_index == 0:
        # Current data-frame will become the last data-frame
        pdf_returned_last = pdf_returned
    else:
        # Concatenate Columns, the result Data Frame contains Timestamp + numpy_array
        pdf_returned = pandas.concat([pdf_returned_last, pdf_returned], axis=0)

        # Current data-frame will become the last data-frame
        pdf_returned_last = pdf_returned

# Save to csv file
print("[INFO] Creating file: %s" % cleaned_data_csv_path)
pdf_returned.to_csv(cleaned_data_csv_path, sep=',', encoding='utf-8', index=False)

print("##############################################")
print("#########      FINISH EXPORTING     ##########")
print("##############################################")
