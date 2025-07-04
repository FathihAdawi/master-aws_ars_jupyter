import re
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from config import db_init
from decimal import Decimal
from pandas.tseries.offsets import MonthEnd

# [SETTING DISPLAY OPTION DATAFRAME]
pd.set_option("display.max_rows", 100, "display.max_columns", 100)

# [VARIABLES DB]
filename = 'db_dwh.ini'
section = 'db'
db_info = db_init(filename, section)

# [CONNECTION & CURSOR]
db_connection = psycopg2.connect(**db_info)
db_cursor = db_connection.cursor()

# """
#     GET ALL TK KEMANDORAN PANEN, LOADING, PERAWATAN, SUPERVISI
# """
# query1 = 'select "ESTATE", "DEVICE ID", "DEVICE NAME", "DEVICE CODE" from "L1_Fact_AWS_ARS";'
# db_cursor.execute(query1)
# df_m_aws_ars = pd.DataFrame(
#     db_cursor.fetchall(),
#     columns=["ESTATE", "DEVICE ID", "DEVICE NAME", "DEVICE CODE"]
# )
#
# list_Device_ID = list(df_m_aws_ars["DEVICE ID"])

list_Device_ID = ['MTI-RPZFM1HPN050']


def API_retrieved_aws_ars():
    while True:
        try:
            # print("=== Custom Retrieved Date IOT AWS ARS ====")
            # v_month = input("Month: ")
            # v_day = input("Day: ")
            # v_year = input("Year: ")
            #
            # custom_EOD = (datetime.strptime(str(v_year + '-' + v_month + '-' + v_day), "%Y-%m-%d") +
            #               pd.offsets.MonthEnd(0)).date()
            # # startOfDate = custom_EOD - timedelta(days=7)
            # custom_SOD = datetime.strptime(str(v_year + '-' + v_month + '-' + v_day), "%Y-%m-%d").date()
            # print("Start Of Date: " + str(custom_SOD) + "\n" + "End Of Date: " + str(custom_EOD))

            for d in list_Device_ID:

                r = requests.get(
                    "http://forwarding.mertani.my.id/pull-sensor-record?deviceId=" + d + "&fromDate=" + str(
                        '2023-11-19') + "&endDate"
                                      "=" + str('2023-11-19') + "&zone=0",
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

                        """
                            Convert unixTime to WIB UTC+07:00 as FullDate_WIB
                        """
                        df_aws_ars['FullDate_WIB'] = pd.to_datetime(
                            df_aws_ars['unixTime'],
                            unit='s'
                        ).dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
                        df_aws_ars['FullDate_WIB'] = df_aws_ars[
                            'FullDate_WIB'
                        ].dt.strftime("%Y-%m-%d %H:%M:%S")

                        """
                            Convert unixTime to WITA UTC+08:00 as FullDate_WITA
                        """
                        df_aws_ars['FullDate_WITA'] = pd.to_datetime(
                            df_aws_ars['unixTime'],
                            unit='s'
                        ).dt.tz_localize('UTC').dt.tz_convert('Asia/Kuala_Lumpur')
                        df_aws_ars['FullDate_WITA'] = df_aws_ars[
                            'FullDate_WITA'
                        ].dt.strftime("%Y-%m-%d %H:%M:%S")

                        """
                            Insert ModifyDatetime where is data created
                        """
                        df_aws_ars['ModifyDateTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        df_aws_ars.rename(columns=lambda x: x.split(' ')[0], inplace=True)
                        df_cols_1 = df_aws_ars[['lat_1', 'long_1', 'batt_1']]
                        cols = df_aws_ars.columns.values
                        new_cols = [re.split(r'_', item)[-1] for item in cols]
                        df_aws_ars.columns = new_cols
                        df_aws_ars.drop(['1'], axis=1, inplace=True)
                        cols_without_1 = df_cols_1.columns.values
                        new_cols_1 = [re.split(r'_', item)[0] for item in cols_without_1]
                        df_cols_1.columns = new_cols_1

                        df_cols_1.rename(
                            columns={'lat': 'Latitude', 'long': 'Longitude', 'batt': 'Battery'},
                            inplace=True
                        )

                        df_aws_ars.rename(
                            columns={'devId': 'DeviceID', 'name': 'DeviceName', 'sig': 'Signal_Val',
                                     'unixTime': 'UnixTime', 'WIB': 'FullDate_WIB', 'WITA': 'FullDate_WITA',
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
                            'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val',
                        ]].applymap(str)

                        m_df_aws_ars = pd.concat([df_cols_1, df_aws_ars], axis=1, join='inner')

                        m_df_aws_ars[[
                            'Longitude', 'Latitude', 'Battery', 'Signal_Val', 'SolarRad_Val', 'WindDir_Val',
                            'AirHmd_Val',
                            'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                        ]] = m_df_aws_ars[[
                            'Longitude', 'Latitude', 'Battery', 'Signal_Val', 'SolarRad_Val', 'WindDir_Val',
                            'AirHmd_Val',
                            'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                        ]].replace({'nan': 0})

                        m_df_aws_ars = m_df_aws_ars.replace({np.nan: None})

                        m_df_aws_ars_2 = m_df_aws_ars[['DeviceID', 'DeviceName', 'FullDate_WIB', 'FullDate_WITA',
                                                       'UnixTime', 'Longitude', 'Latitude', 'Battery', 'Signal_Val',
                                                       'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'WindSpd_Val',
                                                       'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val',
                                                       'RainFal_Val', 'ModifyDateTime']]

                        print(m_df_aws_ars_2[['FullDate_WIB', 'UnixTime', 'Signal_Val']].head())

                        breakpoint()

                        # query_delete_raw = 'delete from "L1_AWS_ARS_RAW" where "FullDate" = \''+str(custom_SOD)+'\';'
                        # db_cursor.execute(query_delete_raw)

                        # tuple_aws_ars = tuple(map(tuple, m_df_aws_ars_2.values))
                        #
                        # query_insert_raw = "insert into \"L1_AWS_ARS_RAW\" (\"DeviceID\", \"DeviceName\", " \
                        #                    "\"FullDate_WIB\", \"FullDate_WITA\", \"UnixTime\", \"Longitude\", " \
                        #                    "\"Latitude\", \"Battery\", \"Signal_Val\", \"SolarRad_Val\", " \
                        #                    "\"WindDir_Val\", \"AirHmd_Val\", \"WindSpd_Val\", \"AirPrs_Val\", " \
                        #                    "\"PhotoActRad_Val\", \"Battery_Val\", \"AirTem_Val\", " \
                        #                    "\"RainFal_Val\", \"ModifyDateTime\") " \
                        #                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        #                    "%s,%s,%s)"
                        #
                        # db_cursor.executemany(
                        #     query_insert_raw,
                        #     tuple_aws_ars
                        # )
                        # db_connection.commit()
                    else:
                        print("Data is empty in " + d)
                else:
                    print("There is no DATA in Device: " + d)

            break
        except ValueError:
            print("Your Format Date Doesn't Match [Year]-[Month]-[day]!\n")


def API_clean_aws_ars():
    # custom_Date = '11-24-2023'
    # query_delete = 'delete from "L2_AWS_ARS" where "FullDate" = \''+custom_Date+'\';'
    # db_cursor.execute(query_delete)
    """
    Cleaning Data Frame After Input Data Raw
    :return:
    """
    query_clean = 'select "DeviceID", "DeviceName", "FullDate_WIB", "FullDate_WITA", "UnixTime", "Longitude", ' \
                  '"Latitude", "Battery", ' \
                  '"Signal_Val", "SolarRad_Val", "WindDir_Val", "AirHmd_Val", "WindSpd_Val", "AirPrs_Val", ' \
                  '"PhotoActRad_Val", "Battery_Val", "AirTem_Val", "RainFal_Val", "ModifyDateTime" from ' \
                  '"L1_AWS_ARS_RAW" where date_part(\'month\', "FullDate_WIB") = 1 and date_part(\'year\', ' \
                  '"FullDate_WIB") = 2023;'
    db_cursor.execute(query_clean)
    df_clean_data = pd.DataFrame(
        db_cursor.fetchall(),
        columns=["DeviceID", "DeviceName", "FullDate", "FullDate_WITA", "UnixTime", "Longitude", "Latitude",
                 "Battery", "Signal_Val",
                 "SolarRad_Val", "WindDir_Val", "AirHmd_Val", "WindSpd_Val", "AirPrs_Val", "PhotoActRad_Val",
                 "Battery_Val", "AirTem_Val", "RainFal_Val", "ModifyDateTime"]
    )

    # print(df_clean_data['Signal_Val'].values)

    # check_Undefined_Val = df_clean_data[['Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val']].apply(lambda
    # x: x.str.contains('undefined'))
    for i, y in df_clean_data[['Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'WindSpd_Val', 'AirPrs_Val',
                               'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val']].items():
        df_clean_data[i] = df_clean_data[i].apply(lambda x: None if 'undefined' in x else x)

    for i, y in df_clean_data[['Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'WindSpd_Val', 'AirPrs_Val',
                               'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val']].items():
        # df_clean_data[i] = df_clean_data[i].str.extract(r'(\d\D+)$')
        df_clean_data[i] = df_clean_data[i].str.extract(r'(\'value\'.+\')')

        # df_clean_data[i] = df_clean_data[i].str.extract(r'(^\d)')

    # if check_Undefined_Val.str.cont == True:
    #     print('There is undefined values!')
    # else:
    #     print('There is no undefined values!')

    # """
    #     Get Digit From List String
    # """
    # df_clean_data['Signal_Val'] = df_clean_data['Signal_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['Signal_Val'] = df_clean_data['Signal_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['SolarRad_Val'] = df_clean_data['SolarRad_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['SolarRad_Val'] = df_clean_data['SolarRad_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['WindDir_Val'] = df_clean_data['WindDir_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['WindDir_Val'] = df_clean_data['WindDir_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['AirHmd_Val'] = df_clean_data['AirHmd_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['AirHmd_Val'] = df_clean_data['AirHmd_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['WindSpd_Val'] = df_clean_data['WindSpd_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['WindSpd_Val'] = df_clean_data['WindSpd_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['AirPrs_Val'] = df_clean_data['AirPrs_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['AirPrs_Val'] = df_clean_data['AirPrs_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['PhotoActRad_Val'] = df_clean_data['PhotoActRad_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['PhotoActRad_Val'] = df_clean_data['PhotoActRad_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['Battery_Val'] = df_clean_data['Battery_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['Battery_Val'] = df_clean_data['Battery_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['AirTem_Val'] = df_clean_data['AirTem_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['AirTem_Val'] = df_clean_data['AirTem_Val'].str.extract(r'(^\d)')
    #
    # df_clean_data['RainFal_Val'] = df_clean_data['RainFal_Val'].str.extract(r'(\d\D+)$')
    # df_clean_data['RainFal_Val'] = df_clean_data['RainFal_Val'].str.extract(r'(^\d)')

    df_clean_data.rename(
        columns={'ModifyDateTime': 'ModifyDate'},
        inplace=True
    )

    df_clean_data.insert(18, "ModifyStatus", "I")

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

    query_insert_clean_raw = "insert into \"L2_AWS_ARS_TEMP\" (\"DeviceID\", \"DeviceName\", \"FullDate\", " \
                             "\"FullDate_WITA\", " \
                             "\"UnixTime\", \"Longitude\", \"Latitude\", \"Battery\", \"Signal_Val\", " \
                             "\"SolarRad_Val\", \"WindDir_Val\", \"AirHmd_Val\", \"WindSpd_Val\", \"AirPrs_Val\", " \
                             "\"PhotoActRad_Val\", \"Battery_Val\", \"AirTem_Val\", \"RainFal_Val\", " \
                             "\"ModifyStatus\", \"ModifyDate\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                             "%s,%s,%s,%s,%s)"

    db_cursor.executemany(
        query_insert_clean_raw,
        tuple_clean_aws_ars
    )
    db_connection.commit()

    # print(df_clean_data.info)


API_retrieved_aws_ars()

# API_clean_aws_ars()
