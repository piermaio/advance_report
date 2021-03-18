import db_connection
import os
import pandas as pd

table_name = 'gw_burntarea_effis.ba_oracle_export_year'

def sum_count(df_sql, df_nations):
    # df_sql, nat2k_year, nat2kweek, df_nations = db_connection.db_connection(table_name)
    df = df_sql
    df['AREA_HA'] = df['AREA_HA'].astype(int)
    # Conversions from string to date format
    df['FIREDATE'] = pd.to_datetime(df['FIREDATE'])
    df['lastfiredate'] = pd.to_datetime(df['lastfiredate'])
    df['LASTUPDATE'] = pd.to_datetime(df['LASTUPDATE'])
    list_eunon = ['EU', 'EU_non', 'ME_AF']
    # print('The dataset will be divided in three macroregions: {}'.format(list_eunon))
    # dataframes definition
    df = df.merge(df_nations, how='inner', left_on='COUNTRY', right_on='NUTS0_CODE')
    # print('\n --- Count of burnt areas per EU, nonEU, ME_NA --- \n')
    # print(df.columns)
    df_eu = df.loc[df['EU_nonEU'] == 'EU']
    cut_labels = ['>=50 ha but <100', '>=100 and <500 ha', '>=500 and <1000 ha', '>=1000 ha']
    cut_bins = [50, 100, 500, 1000, 1000000000]
    df_eu['bin'] = pd.cut(df_eu['AREA_HA'], bins=cut_bins, labels=cut_labels)
    # df_eu.to_csv('df_eu.csv')
    groups = df_eu.groupby(['NUTS_NAME', 'bin']).agg({'AREA_HA': ['sum', 'count']})
    groups.to_csv('4_ba_by_fire_size_class_in_eu.csv')

def main(df_sql, df_nations):
    sum_count(df_sql, df_nations)

if __name__ == '__main__':
    main()