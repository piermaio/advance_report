import db_connection
import os
import pandas as pd

table_name = 'gw_burntarea_effis.ba_oracle_export_year'

def sum_count(path):
    df_sql, nat2k_year, nat2kweek, df_nations = db_connection.db_connection(table_name)
    os.chdir(path)
    df = df_sql
    df['AREA_HA'] = df['AREA_HA'].astype(int)
    # Conversions from string to date format
    df['FIREDATE'] = pd.to_datetime(df['FIREDATE'])
    df['lastfiredate'] = pd.to_datetime(df['lastfiredate'])
    df['LASTUPDATE'] = pd.to_datetime(df['LASTUPDATE'])
    list_eunon = ['EU', 'EU_non', 'ME_AF']
    print('The dataset will be divided in three macroregions: {}'.format(list_eunon))
    # dataframes definition
    df = df.merge(df_nations, how='inner', left_on='COUNTRY', right_on='NUTS0_CODE')
    print('\n --- Count of burnt areas per EU, nonEU, ME_NA --- \n')
    print(df.columns)
    df3 = pd.DataFrame(df.groupby([pd.Grouper(key='EU_nonEU')])['AREA_HA'].sum())
    df2 = pd.DataFrame(df.groupby([pd.Grouper(key='NUTS_NAME')]).agg({'AREA_HA': ['sum', 'count']}))
    df2.loc["Total"] = df2.sum()
    # df_eu = df.loc[df['EU_nonEU'] == 'EU']
    # df_eunon = df.loc[df['EU_nonEU'] == 'EU_non']
    # df_meaf = df.loc[df['EU_nonEU'] == 'ME_AF']
    print(df2)
    df2.to_csv('1_burnt_areas_sum_count_by_country.csv')
    df3.to_csv('1b_burnt_areas_sum_count_by_country.csv')
    return 0