# [version: 1.0.5]
import re
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from configure import db_init
import time

# [SETTING DISPLAY OPTION DATAFRAME]
pd.set_option("display.max_rows", 100, "display.max_columns", 100)

# [VARIABLES DB]
filename = 'D:\All_app_code\Testing Apps\master-aws_ars_jupyter\db_dwh.ini'
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

list_device_id = list(df_m_aws_ars["DEVICE ID"])
# list_Device_ID = ['MTI-163KPI7MDBKL']
print("Total DeviceID: "+str(len(list_device_id)))
start_time = time.time()

def api_retrieved_aws_ars():

    query_delete_raw = 'delete from "L1_AWS_ARS_RAW" where "FullDate_WIB"::date > current_date - interval \'30 days\' ' \
                        'and "FullDate_WIB"::date <= current_date + interval \'1 day\';'    
    db_cursor.execute(query_delete_raw)

    while True:
        try:
            print("=== Custom Retrieved Date IOT AWS ARS ====")
            custom_eod = datetime.today().date() + timedelta(days=1)
            custom_sod = custom_eod - timedelta(days=30)
            print("Start_Date: " + str(custom_sod) + "\nEnd_Date: " + str(custom_eod))
            

            for d in list_device_id:

                r = requests.get(
                    "http://forwarding.mertani.my.id/pull-sensor-record?deviceId=" + d + "&fromDate=" + str(
                        str(custom_sod)) + "&endDate"
                                      "=" + str(custom_eod) + "&zone=0",
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

                        df_aws_ars['unixTime'] = df_aws_ars['unixTime'].astype(int)

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

                        df_cols_2 = df_cols_1.rename(
                            columns={'lat': 'Latitude', 'long': 'Longitude', 'batt': 'Battery'}
                        )

                        df_aws_ars.rename(
                            columns={'devId': 'DeviceID', 'name': 'DeviceName', 'sig': 'Signal_Val',
                                     'unixTime': 'UnixTime', 'WIB': 'FullDate_WIB', 'WITA': 'FullDate_WITA',
                                     'slrRad': 'SolarRad_Val', 'winDir': 'WindDir_Val', 'arHum': 'AirHmd_Val',
                                     'winSpe': 'WindSpd_Val', 'arPre': 'AirPrs_Val', 'par': 'PhotoActRad_Val',
                                     'batt': 'Battery_Val', 'arTem': 'AirTem_Val', 'rnFal': 'RainFal_Val'},
                            inplace=True
                        )

                        df_cols_2[[
                            'Latitude', 'Longitude', 'Battery'
                        ]] = df_cols_2[[
                            'Latitude', 'Longitude', 'Battery'
                        ]].astype(str)

                        df_aws_ars[[
                            'Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'UnixTime',
                            'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                        ]] = df_aws_ars[[
                            'Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'UnixTime',
                            'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val',
                        ]].applymap(str)

                        m_df_aws_ars = pd.concat([df_cols_2, df_aws_ars], axis=1, join='inner')

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

                        tuple_aws_ars = tuple(map(tuple, m_df_aws_ars_2.values))
                        
                        query_insert_raw = "insert into \"L1_AWS_ARS_RAW\" (\"DeviceID\", \"DeviceName\", " \
                                           "\"FullDate_WIB\", \"FullDate_WITA\", \"UnixTime\", \"Longitude\", " \
                                           "\"Latitude\", \"Battery\", \"Signal_Val\", \"SolarRad_Val\", " \
                                           "\"WindDir_Val\", \"AirHmd_Val\", \"WindSpd_Val\", \"AirPrs_Val\", " \
                                           "\"PhotoActRad_Val\", \"Battery_Val\", \"AirTem_Val\", " \
                                           "\"RainFal_Val\", \"ModifyDateTime\") " \
                                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                           "%s,%s,%s)"
                        
                        db_cursor.executemany(
                            query_insert_raw,
                            tuple_aws_ars
                        )
                        db_connection.commit()
                    else:
                        print("Data is empty in " + d)
                else:
                    print("There is no DATA in Device: " + d)
            break
        except ValueError:
            print("Your Format Date Doesn't Match [Year]-[Month]-[day]!\n")

def api_clean_aws_ars():
    
    """
        Cleaning Data Frame After Input Data Raw
        :return:
    """

    # [Delete filter from L2_AWS_ARS Table]
    query_delete_clean = 'delete from "L2_AWS_ARS" where "FullDate"::date > current_date - interval \'30 days\' ' \
                        'and "FullDate"::date <= current_date + interval \'1 day\';' 
    db_cursor.execute(query_delete_clean)

    # [Get Data from L1_AWS_ARS_RAW Table]
    query_clean = 'select "DeviceID", "DeviceName", "FullDate_WIB", "FullDate_WITA", "UnixTime", "Longitude", ' \
                '"Latitude", "Battery", "Signal_Val", "SolarRad_Val", "WindDir_Val", "AirHmd_Val", ' \
                '"WindSpd_Val", "AirPrs_Val", "PhotoActRad_Val", "Battery_Val", "AirTem_Val", ' \
                '"RainFal_Val", "ModifyDateTime" from "L1_AWS_ARS_RAW" ' \
                'where "FullDate_WIB"::date > current_date - interval \'30 days\' ' \
                'and "FullDate_WIB"::date <= current_date + interval \'1 day\';'                
    db_cursor.execute(query_clean)
    df_clean_data = pd.DataFrame(
        db_cursor.fetchall(),
        columns=["DeviceID", "DeviceName", "FullDate", "FullDate_WITA", "UnixTime", "Longitude", "Latitude",
                 "Battery", "Signal_Val", "SolarRad_Val", "WindDir_Val", "AirHmd_Val", "WindSpd_Val", 
                 "AirPrs_Val", "PhotoActRad_Val", "Battery_Val", "AirTem_Val", "RainFal_Val", "ModifyDateTime"]
    )


    for i in df_clean_data[['Signal_Val', 'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val', 'WindSpd_Val', 'AirPrs_Val',
                               'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val']].columns:
        df_clean_data[i] = df_clean_data[i].apply(lambda x: None if 'undefined' in x else x)
        df_clean_data[i] = df_clean_data[i].str.extract(r'(\'value\'.+\')')
        df_clean_data[i] = df_clean_data[i].str.replace(r'[\'value\':\s\']', '', regex=True)

    df_clean_data.rename(
        columns={'ModifyDateTime': 'ModifyDate'},
        inplace=True
    )

    df_clean_data.insert(18, "ModifyStatus", "I")

    df_clean_data[['Latitude', 'Longitude', 'Battery', 'Signal_Val', 
                   'SolarRad_Val', 'WindDir_Val', 'AirHmd_Val',
                   'WindSpd_Val', 'AirPrs_Val', 'PhotoActRad_Val', 
                   'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                   ]] = df_clean_data[['Latitude', 'Longitude', 'Battery', 'Signal_Val', 'SolarRad_Val', 
                                       'WindDir_Val', 'AirHmd_Val', 'WindSpd_Val', 'AirPrs_Val', 
                                       'PhotoActRad_Val', 'Battery_Val', 'AirTem_Val', 'RainFal_Val'
                                       ]].astype(float)

    tuple_clean_aws_ars = tuple(map(tuple, df_clean_data.values))

    query_insert_clean_raw = "insert into \"L2_AWS_ARS\" (\"DeviceID\", \"DeviceName\", \"FullDate\", " \
                            "\"FullDate_WITA\", \"UnixTime\", \"Longitude\", \"Latitude\", \"Battery\", "\
                            "\"Signal_Val\", \"SolarRad_Val\", \"WindDir_Val\", \"AirHmd_Val\", \"WindSpd_Val\", " \
                            "\"AirPrs_Val\", \"PhotoActRad_Val\", \"Battery_Val\", \"AirTem_Val\", \"RainFal_Val\", " \
                            "\"ModifyStatus\", \"ModifyDate\") " \
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    db_cursor.executemany(
        query_insert_clean_raw,
        tuple_clean_aws_ars
    )
    db_connection.commit()

    print("Success To Clean!")

def api_refresh_mv_l3():

    """
        Serve all refresh materialized view for dashboard needs
        :return:
    """
    list_mv_l3 = ['refresh materialized view public.mv_l3_iot_aws;', 
                  'refresh materialized view public.mv_l3_aws_unitday;', 
                  'refresh materialized view public.mv_l3_aws_unithour;', 
                  'refresh materialized view public.mv_l3_aws_unitinterval;'
                  ]
    
    for i in list_mv_l3:
        db_cursor.execute(i)
        db_connection.commit()
        print("Success " + i)


# [Execute Function RAW and Clean Data]
api_retrieved_aws_ars()
api_clean_aws_ars()
api_refresh_mv_l3()

# [Get Runtime Execution Process]
end_time = time.time() - start_time
final_end_time = end_time / 60
print("--- %s minutes ---" % final_end_time)
