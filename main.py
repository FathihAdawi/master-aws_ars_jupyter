import re
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from config import db_init
from decimal import Decimal

# [SETTING DISPLAY OPTION DATAFRAME]
pd.set_option("display.max_rows", 100, "display.max_columns", 100)

# [VARIABLES DB]
filename = 'db_dwh.ini'
section = 'tpadw_db'
db_info = db_init(filename, section)

# [CONNECTION & CURSOR]
db_connection = psycopg2.connect(**db_info)
db_cursor = db_connection.cursor()

"""
    GET ALL TK KEMANDORAN PANEN, LOADING, PERAWATAN, SUPERVISI
"""
query1 = 'select "ESTATE", "DEVICE ID", "DEVICE NAME", "DEVICE CODE" from "L1_Fact_AWS_ARS";'
db_cursor.execute(query1)
df_m_aws_ars = pd.DataFrame(
    db_cursor.fetchall(),
    columns=["ESTATE", "DEVICE ID", "DEVICE NAME", "DEVICE CODE"]
)

list_Device_ID = list(df_m_aws_ars["DEVICE ID"])

# list_device_ID = ['MTI-83S0J5D41G3J', 'MTI-YG6OJJFOAQNQ',
#                   'MTI-DFL8XBD9SCNU',
#                   'MTI-M5Q3Z72OJFSH',
#                   'MTI-WFHP8QGEYKEV',
#                   'MTI-GDMCROEJFZFT',
#                   'MTI-XJ0N6L6I3CMI',
#                   'MTI-GKD0K1A92AH7',
#                   'MTI-XPAY1JZ898Y6']


def API_retrieved_aws_ars():
    endOfDate = datetime.today().date()
    startOfDate = datetime.today().date() - timedelta(days=7)
    print("Start Of Date: " + str(startOfDate) + "\n" + "Before Of Date: " + str(endOfDate))

    # query_delete = 'delete from "L1_Fact_AWS_ARS" where "ModifyDateTime" between ' \
    #                + str(startOfDate) + ' and ' + str(endOfDate) + ';'
    # db_cursor.execute(query_delete)

    for d in list_Device_ID:

        r = requests.get(
            "http://forwarding.mertani.my.id/pull-sensor-record?deviceId=" + d + "&fromDate=" + str(
                startOfDate) + "&endDate"
                               "=" + str(endOfDate) + "&zone=0",
            headers={"Token": "xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxx"}
        )

        data = json.loads(r.text)
        if "data" in data:
            if bool(data["data"]):
                print(d)
                df_aws_ars = pd.DataFrame(data["data"])
                df_aws_ars.reset_index(
                    drop=True,
                    inplace=True
                )

                df_aws_ars['FullDate'] = df_aws_ars['unixTime'].astype(int)
                df_aws_ars['FullDate'] = pd.to_datetime(df_aws_ars['FullDate'], unit='s')
                df_aws_ars['ModifyDateTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                df_aws_ars.rename(columns=lambda x: x.split(' ')[0], inplace=True)
                df_cols_1 = df_aws_ars[['lat_1', 'long_1', 'batt_1']]

                cols = df_aws_ars.columns.values
                new_cols = [re.split(r'_', item)[-1] for item in cols]
                df_aws_ars.columns = new_cols
                df_aws_ars.drop(['1'], axis=1, inplace=True)
                print(df_aws_ars.info())
                print(df_cols_1)
                breakpoint()
                cols_without_1 = df_cols_1.columns.values
                new_cols_1 = [re.split(r'_', item)[0] for item in cols_without_1]
                df_cols_1.columns = new_cols_1
                df_cols_1.rename(
                    columns={'lat': 'Latitude', 'long': 'Longitude', 'batt': 'Battery'},
                    inplace=True
                )

                df_aws_ars.rename(
                    columns={'devId': 'DeviceID', 'name': 'DeviceName', 'sig': 'Signal_Val', 'unixTime': 'UnixTime',
                             'slrRad': 'SolarRad_Val', 'winDir': 'WindDir_Val', 'arHum': 'AirHmd_Val',
                             'winSpe': 'WindSpd_Val', 'arPre': 'AirPrs_Val', 'par': 'PhotoActRad_Val',
                             'batt': 'Battery_Val', 'arTem': 'AirTem_Val', 'rnFal': 'RainFal_Val'},
                    inplace=True
                )

                df_cols_1[[
                    'Latitude', 'Longitude', 'Battery'
                ]] = df_cols_1[[
                    'Latitude', 'Longitude', 'Battery'
                ]].astype(str)

                df_aws_ars[[
                    'Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'UnixTime',
                    'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                ]] = df_aws_ars[[
                    'Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'UnixTime',
                    'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                ]].applymap(str)

                m_df_aws_ars = pd.concat([df_cols_1, df_aws_ars], axis=1, join='inner')

                m_df_aws_ars[[
                    'Longitude', 'Latitude', 'Battery', 'Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val',
                    'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                ]] = m_df_aws_ars[[
                    'Longitude', 'Latitude', 'Battery', 'Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val',
                    'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                ]].replace({'nan': 0})

                m_df_aws_ars = m_df_aws_ars.replace({np.nan: None})

                m_df_aws_ars_2 = m_df_aws_ars[['DeviceID', 'DeviceName', 'FullDate', 'UnixTime', 'Longitude',
                                               'Latitude', 'Battery', 'Signal_Val', 'SolarRad_Val', 'WindDir_Val',
                                               'AirHmd_Val', 'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val',
                                               'Battery_Val', 'AirTem_Val', 'RainFal_Val', 'ModifyDateTime']]

                tuple_aws_ars = tuple(map(tuple, m_df_aws_ars_2.values))

                query_insert_raw = "insert into \"L1_AWS_ARS_RAW\" (\"DeviceID\", \"DeviceName\", \"FullDate\", " \
                                   "\"UnixTime\", \"Longitude\", \"Latitude\", \"Battery\", \"Signal_Val\", " \
                                   "\"SolarRad_Val\", " \
                                   "\"WindDir_Val\", \"AirHmd_Val\", \"WindSpd_Val\", \"AirPrs_Val\", " \
                                   "\"PhotoActRad_Val\", \"Battery_Val\", \"AirTem_Val\", \"RainFal_Val\", " \
                                   "\"ModifyDateTime\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                db_cursor.executemany(
                    query_insert_raw,
                    tuple_aws_ars
                )
                db_connection.commit()
            else:
                print("Data is empty in " + d)
        else:
            print("There is no DATA in Device: " + d)


def API_clean_aws_ars():
    """
    Cleaning Data Frame After Input Data Raw
    :return:
    """
    query_clean = 'select "DeviceID", "DeviceName", "FullDate", "UnixTime", "Longitude", "Latitude", "Battery", ' \
                  '"Signal_Val", "SolarRad_Val", "WindDir_Val", "AirHmd_Val", "WindSpd_Val", "AirPrs_Val", ' \
                  '"PhotoActRad_Val", "Battery_Val", "AirTem_Val", "RainFal_Val", "ModifyDateTime" from ' \
                  '"L1_AWS_ARS_RAW";'
    db_cursor.execute(query_clean)
    df_clean_data = pd.DataFrame(
        db_cursor.fetchall(),
        columns=["DeviceID", "DeviceName", "FullDate", "UnixTime", "Longitude", "Latitude", "Battery", "Signal_Val",
                 "SolarRad_Val", "WindDir_Val", "AirHmd_Val", "WindSpd_Val", "AirPrs_Val", "PhotoActRad_Val",
                 "Battery_Val", "AirTem_Val", "RainFal_Val", "ModifyDateTime"]
    )

    """
        Get Digit From List String
    """
    df_clean_data['Signal_Val'] = df_clean_data['Signal_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['Signal_Val'] = df_clean_data['Signal_Val'].str.extract(r'(^\d)')

    df_clean_data['SolarRad_Val'] = df_clean_data['SolarRad_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['SolarRad_Val'] = df_clean_data['SolarRad_Val'].str.extract(r'(^\d)')

    df_clean_data['WindDir_Val'] = df_clean_data['WindDir_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['WindDir_Val'] = df_clean_data['WindDir_Val'].str.extract(r'(^\d)')

    df_clean_data['AirHmd_Val'] = df_clean_data['AirHmd_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['AirHmd_Val'] = df_clean_data['AirHmd_Val'].str.extract(r'(^\d)')

    df_clean_data['WindSpd_Val'] = df_clean_data['WindSpd_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['WindSpd_Val'] = df_clean_data['WindSpd_Val'].str.extract(r'(^\d)')

    df_clean_data['AirPrs_Val'] = df_clean_data['AirPrs_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['AirPrs_Val'] = df_clean_data['AirPrs_Val'].str.extract(r'(^\d)')

    df_clean_data['PhotoActRad_Val'] = df_clean_data['PhotoActRad_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['PhotoActRad_Val'] = df_clean_data['PhotoActRad_Val'].str.extract(r'(^\d)')

    df_clean_data['Battery_Val'] = df_clean_data['Battery_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['Battery_Val'] = df_clean_data['Battery_Val'].str.extract(r'(^\d)')

    df_clean_data['AirTem_Val'] = df_clean_data['AirTem_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['AirTem_Val'] = df_clean_data['AirTem_Val'].str.extract(r'(^\d)')

    df_clean_data['RainFal_Val'] = df_clean_data['RainFal_Val'].str.extract(r'(\d\D+)$')
    df_clean_data['RainFal_Val'] = df_clean_data['RainFal_Val'].str.extract(r'(^\d)')

    df_clean_data.rename(
        columns={'ModifyDateTime': 'ModifyDate'},
        inplace=True
    )

    df_clean_data.insert(17, "ModifyStatus", "I")

    df_clean_data[['Latitude', 'Longitude', 'Battery', 'Signal_Val',
                   'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val',
                   'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val',
                   'Battery_Val', 'AirTem_Val',
                   'RainFal_Val'
                   ]] = df_clean_data[['Latitude', 'Longitude', 'Battery', 'Signal_Val',
                                       'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val',
                                       'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val',
                                       'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                                       ]].astype(float)

    tuple_clean_aws_ars = tuple(map(tuple, df_clean_data.values))
    query_insert_clean_raw = "insert into \"L2_AWS_ARS\" (\"DeviceID\", \"DeviceName\", \"FullDate\", " \
                             "\"UnixTime\", \"Longitude\", \"Latitude\", \"Battery\", \"Signal_Val\", " \
                             "\"SolarRad_Val\", \"WindDir_Val\", \"AirHmd_Val\", \"WindSpd_Val\", \"AirPrs_Val\", " \
                             "\"PhotoActRad_Val\", \"Battery_Val\", \"AirTem_Val\", \"RainFal_Val\", " \
                             "\"ModifyStatus\", \"ModifyDate\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                             "%s,%s,%s,%s)"

    db_cursor.executemany(
        query_insert_clean_raw,
        tuple_clean_aws_ars
    )
    db_connection.commit()

    print(df_clean_data.info)


API_retrieved_aws_ars()
# API_clean_aws_ars()
