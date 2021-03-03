import os
import pandas as pd
import psycopg2
import datetime as dt
import numpy as np
import sqlalchemy
from rasterstats import zonal_stats
import geopandas as gpd
import csv



def corine_stats(root, ba_ref, burnt_areas, corine_path):
    # dict_path = 'C:\\Users\\piermaio\\Documents\\gisdata\\jrc\\advance_report'  # PM local path to the corine key
    countries_dict = {
        'AL': 'Albania', 'DZ': 'Algeria', 'AM': 'Armenia', 'AT': 'Austria', 'AZ': 'Azerbaijan', 'BE': 'Belgium',
        'BA': 'Bosnia and Herzegovina', 'BG': 'Bulgaria', 'BY': 'Byelorussia', 'HR': 'Croatia', 'CY': 'Cyprus',
        'CZ': 'Czech republic', 'DK': 'Denmark', 'EG': 'Egypt', 'IE': 'Ireland', 'EE': 'Estonia', 'FI': 'Finland',
        'FR': 'France', 'GE': 'Georgia', 'DE': 'Germany', 'GR': 'Greece', 'NL': 'The Netherlands', 'HU': 'Hungary',
        'IR': 'Iran', 'IQ': 'Iraq', 'IL': 'Israel', 'IT': 'Italy', 'JO': 'Jordan', 'KZ': 'Kazakhstan', 'LV': 'Latvia',
        'LB': 'Lebanon', 'LY': 'Libya', 'LT': 'Lithuania', 'LU': 'Luxemburg', 'MD': 'Moldavia', 'ME': 'Montenegro',
        'MA': 'Morocco', 'NO': 'Norway', 'PL': 'Poland', 'PT': 'Portugal', 'RO': 'Romania', 'RU': 'Russia',
        'SA': 'Saudi Arabia', 'RS': 'Serbia', 'SK': 'Slovakia', 'SI': 'Slovenia', 'ES': 'Spain', 'SE': 'Sweden',
        'CH': 'Switzerland', 'SY': 'Syria', 'XA': "'The territories'", 'TN': 'Tunisia', 'TR': 'Turkey',
        'TM': 'Turkmenistan', 'UK': 'United Kingdom', 'UZ': 'Uzbekistan', 'IS': 'Island',
        'KS': 'Kosovo under UNSCR 1244', 'GG': 'GUERNSEY', 'MT': 'Malta', 'PS': 'Palestinian Territory',
        'AD': 'Andorra', 'MK': 'North Macedonia', 'UA': 'Ukraine', 'EL': 'Greece'
    }
    os.chdir(root)
    print(ba_ref)
    ba = burnt_areas
    # print(ba['AREA_HA'].head())
    # ba = ba.merge(tab_nations, how='inner', left_on='COUNTRY', right_on='NUTS0_CODE')
    # print('--- Coulmns of burnt areas ---')
    # print(ba.columns)
    # ba = ba.sort_values('id')
    ba.to_csv('corine_ba.csv')
    # ba = ba.reset_index()
    # ba =ba.set_index('id')
    f = csv.reader(open('corine_key.csv'))
    corine_dict = {}
    for row in f:
        corine_dict[row[0]] = row[1]
    corine_dict['1'] = corine_dict.pop('ï»¿1')
    corine_dict = {int(k): v for k, v in corine_dict.items()}
    ba_raw = zonal_stats(ba, corine_path, categorical=True)
    df_ba_raw = pd.DataFrame.from_dict(ba_raw)
    df_ba_raw = df_ba_raw.rename(columns=corine_dict)
    df_ba_raw = df_ba_raw.T
    # print(df_ba_raw.columns)
    df_ba_raw.to_csv('corine_ba_raw.csv')
    df_mapped = df_ba_raw.groupby(df_ba_raw.index).sum()
    df_mapped = df_mapped.T
    df_mapped.to_csv('corine_ba_raw2.csv')
    # df = df_ba_raw.groupby(df_ba_raw[[])['AREA_HA'].sum()
    # df.to_cs
    df = df_mapped
    l = ba['id'].to_list()
    a = ba['AREA_HA'].to_list()
    df = df.assign(id=l, area=a)
    # df.to_csv('__df_test.csv')
    df_merge = df.merge(ba[['id','COUNTRY'
                                              '']], how='inner', on='id')
    # print(df2.columns)
    # print('\n----- df merge columns\n')
    # print(df_merge.columns)
    # df_merge.to_csv('__test_merge.csv')
    df_group = df_merge.groupby([
        'COUNTRY']).sum()
    # df_group.to_csv('__test_group.csv')
    print('\n----- df group columns\n')
    print(df_group.columns)
    df_group = df_group.drop([0, 'id', 'area'], axis=1)
    df_group = df_group.reset_index()
    df_plt = df_group.copy()
    print('\n----- df plt columns\n')
    print(df_plt.columns)
    # df_plt = df_group.drop(['COUNTRYFUL'], axis=1)
    df_tab = df_group.copy()
    # df_tab.drop(['COUNTRYFUL'], axis=1)
    # df_tab.set_index('COUNTRY')
    # df_tab.loc["total"] = df.sum()
    df_perc = df_tab.copy()
    df_perc["total"] = df_perc.sum(axis=1)
    df_perc = df_perc.set_index('COUNTRY')
    list_columns = [x for x in df_perc.columns]
    print(list_columns)
    df_perc.loc[:, list_columns] = df_perc.loc[:, list_columns].div(df_perc["total"], axis=0)
    df_perc.to_csv('_test.csv')
    # print(ba_ref.columns)
    l = tuple(list((ba_ref['AREA_HA']['sum'])))
    print(len(l))
    print(df_perc.shape)
    perc_columns = [x for x in df_perc.columns]
    for i in perc_columns:
        df_perc[i] = df_perc[i].multiply(l)
    df_tab_corrected = df_perc
    # df_tab_corrected.to_csv('7b_landcover_by_country_area.csv')
    # df_tab.to_csv('_test.csv')
    df_countries = pd.DataFrame.from_dict(countries_dict, orient='index')
    df_countries = df_countries.reset_index()
    df_m = df_tab_corrected.merge(df_countries, how='inner', left_on='COUNTRY', right_on='index')
    df_m.to_csv('8_corine_by_countries.csv')

    # print('---- #PM df_plt columns\n')
    # print(df_plt.columns)
    df_plt = df_plt.set_index('COUNTRY')
    # df_plt.drop(NaN)
    plot = df_plt.plot.bar(stacked=True, title='Land cover classes affected by burnt areas (ha)\n', figsize=[15,10], colormap='Set2')
    fig = plot.get_figure()
    fig.savefig('8_corine_by_countries.png')
    return df_tab_corrected