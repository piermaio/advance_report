import os
import pandas as pd
import psycopg2
import datetime as dt
import numpy as np
import sqlalchemy
from rasterstats import zonal_stats
import geopandas as gpd
import csv



def corine_stats(df_tab_corine):
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
    df_countries = pd.DataFrame.from_dict(countries_dict, orient='index', columns=['country'])
    df = df_tab_corine
    # print(df_tab.columns)
    # df.set_index(['COUNTRY'])
    # df.drop('COUNTRY', axis=1)
    # df.drop(40, axis=0)
    # df["total"] = df.sum(axis=1)
    # print('---- PM df\n')
    # print(df)
    # df.to_csv('_test.csv')
    # df = df.drop('COUNTRYFUL', axis=1)
    # df = df.set_index('COUNTRYFUL')
    df_area = df.copy()
    df_perc = df.copy()
    list_columns = [x for x in df_perc.columns]
    # print(list_columns)
    # print(df_perc)
    df_perc.loc[:, list_columns] = df_perc.loc[:, list_columns].div(df_perc["total"], axis=0) * 100
    df_perc = df_perc[list_columns].round(2)
    df_perc.to_csv('7a_landcover_by_country_percentage.csv')
    df_area.to_csv('7b_landcover_by_country_area.csv')
    df_merge = df_area.merge(df_perc, how='inner', left_index=True, right_index=True, suffixes=['_area', '_perc'])\
        .merge(df_countries, how='inner', left_index=True, right_index=True)
    df_merge.to_csv('7c_landcover_by_country_area&perc.csv')

    # Comparison with historical data
    # df_hist = pd.read_csv('ba2000_2019.csv')
    # df_group = df_hist.groupby(['Country', 'YearSeason']).sum('Area_HA')



def main(df_tab_corine):
    corine_stats(df_tab_corine)
    pass

if __name__ == '__main__':
    main()