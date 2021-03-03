import db_connection
import os
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
from PIL import Image

table_name = 'gw_burntarea_effis.ba_oracle_export_year'


def dates_range_set(years):
    d = {}
    for i in years:
        first_day = dt.datetime.strptime("01-01-{}".format(i), "%d-%m-%Y")
        dates = []
        for j in (np.arange(53)):
            days = 7 * j
            day = first_day + dt.timedelta(int(days))
            dates.append(day)
        d["range_dates_{}".format(i)] = dates
    return d


def get_text_positions(x_data, y_data, txt_width, txt_height):
    a = list(zip(y_data, x_data))
    text_positions = y_data.copy()
    for index, (y, x) in enumerate(a):
        local_text_positions = [i for i in a if i[0] > (y - txt_height)
                                and (abs(i[1] - x) < txt_width * 3.5) and i != (y, x)]
        if local_text_positions:
            sorted_ltp = sorted(local_text_positions)
            if abs(sorted_ltp[0][0] - y) < txt_height:  # True == collision
                differ = np.diff(sorted_ltp, axis=0)
                a[index] = (sorted_ltp[-1][0] + txt_height, a[index][1])
                text_positions[index] = sorted_ltp[-1][0] + txt_height
                for k, (j, m) in enumerate(differ):
                    # j is the vertical distance between words
                    if j > txt_height * 2.5:  # if True then room to fit a word in
                        a[index] = (sorted_ltp[k][0] + txt_height, a[index][1])
                        text_positions[index] = sorted_ltp[k][0] + txt_height
                        break
    return text_positions


def text_plotter(x_data, y_data, text_positions, axis, txt_width, txt_height):
    for x, y, t in zip(x_data, y_data, text_positions):
        axis.text(x - .03, 1.02 * t, '%d' % int(y), rotation=0, color='black', fontsize=13)
        if y != t:
            axis.arrow(x, t + 20, 0, y - t, color='blue', alpha=0.6, width=txt_width * 0.0,
                       head_width=.02, head_length=txt_height * 0.5,
                       zorder=0, length_includes_head=True)


def labeled_graphs(path, df_cumulative, title, png_name):
    print('Plotting marked and labeled graphs')
    font = {'family': 'serif',
            'color': 'black',
            'weight': 'normal',
            'size': 20,
            }
    df_cumulative = df_cumulative.truncate(after=(dt.datetime.now() - dt.timedelta(days=0)).strftime("%Y-%m-%d"))
    # print("\n Df cumulative slide 5 truncate")
    # print(df_cumulative)
    # Original labeling
    # df_cumulative['Area_HA'].plot(legend=False, figsize=[10,7], color='r', marker='o') # figsize=[8,4],
    # plt.title('2020 burnt area EU countries', fontdict=font)
    # plt.grid(color='grey', linestyle='-', linewidth=1)
    # plt.ylabel('Area (ha)', fontdict=font)
    # plt.xlabel('Time', fontdict=font)
    # area_labels = df_cumulative['Area_HA'].astype(int).apply(lambda x: "{:,}".format(x))
    # for a, b, c in zip(df_cumulative.index, df_cumulative['Area_HA'], area_labels):
    # 	label = "{}".format(c)
    # 	plt.annotate(label,  # this is the text
    # 				 (a, b),  # this is the point to label
    # 				 textcoords="offset points",  # how to position the text
    # 				 xytext=(0, 10),  # distance from text to points (x,y)
    # 				 ha='center')  # horizontal alignment can be left, right or center
    # Adaptive labeling
    fig = plt.figure(figsize=[14, 9])
    ax = fig.add_subplot(111)
    x_data = np.arange(len(df_cumulative.index))
    y_data = df_cumulative['Area_HA']
    ax.plot(x_data, y_data, color='red', marker='o');
    ax.grid(color='grey', linestyle='-', linewidth=1, alpha=0.2)
    plt.title(title, fontdict=font)
    plt.ylabel('Area (ha)', fontdict=font)
    plt.xlabel('Time', fontdict=font)
    # ticks
    dates_label = df_cumulative.index.strftime("%d %b")
    plt.xticks(ticks=x_data, labels=dates_label, rotation='vertical')
    # labels on markers
    txt_height = 0.04 * (plt.ylim()[1] - plt.ylim()[0])
    txt_width = 0.02 * (plt.xlim()[1] - plt.xlim()[0])
    marker_labels = df_cumulative['Area_HA'].astype(int).apply(lambda x: "{:,}".format(x))
    text_positions = get_text_positions(x_data, y_data, txt_width, txt_height)
    text_plotter(x_data, y_data, text_positions, plt, txt_width, txt_height)
    # end of labeling
    plt.savefig(png_name)
    im = Image.open(path + png_name)
    # im.show()
    plt.clf()


def unlabeled_graphs(path, df_comparison, title, png_name, ylabel):
    print('Plotting unlabeled graphs')
    plt.clf()
    ylabel = ylabel
    font = {'family': 'serif',
            'color': 'black',
            'weight': 'normal',
            'size': 20,
            }
    df_comparison = df_comparison.rename(
        columns={"Area_HA": "2020 burnt area", "hist_average_from_2008": "2008-2019 average burnt area"})
    df_comparison = df_comparison.truncate(after=(dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
    # print(df_comparison)
    df_comparison['2020 burnt area'].plot(legend=True, figsize=[10, 7], color='r')
    df_comparison['2008-2019 average burnt area'].plot(legend=True, figsize=[10, 7], color='b')
    plt.ylabel(ylabel, fontdict=font)
    plt.xlabel('Time', fontdict=font)
    plt.title(title, fontdict=font)
    plt.grid(color='grey', linestyle='-', linewidth=1)
    plt.savefig(png_name)
    from PIL import Image
    im = Image.open(path + png_name)
    # im.show()
    plt.clf()


def sum_count(path, df_sql, df_nations):
    # df_sql, gdf_sql, nat2k_year, nat2kweek, df_nations = db_connection.db_connection(table_name)
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
    df_eu = df.loc[df['EU_nonEU'] == 'EU']
    df_eunon = df.loc[df['EU_nonEU'] == 'EU_non']
    df_meaf = df.loc[df['EU_nonEU'] == 'ME_AF']
    df_eunon_meaf = pd.concat([df_eunon, df_meaf])
    df_history = pd.read_csv('ba_2008_2019.csv')
    df_history = df_history.loc[
        df_history['Area_HA'] >= 30]  # areas over 30ha to harmonize the comparison with historical data
    df_history['FireDate'] = pd.to_datetime(df_history['FireDate'])
    df_history = df_history.merge(df_nations, how='inner', left_on='Country', right_on='NUTS0_CODE')

    # Creating the bins and the dates tags
    years = np.arange(2008, 2021)  # right limit excluded
    range = dates_range_set(years)
    range_2020 = pd.DataFrame(range.get('range_dates_2020'))
    range_2020.columns = ['bins']
    dates_tag = []
    for i in range_2020['bins']:
        k = str(i)[0:10]
        dates_tag.append(dt.datetime.strptime(k, '%Y-%m-%d'))
    # print(dates_tag)

    # Burnt areas weekly evolution in EU during the year
    df_area = pd.DataFrame(
        df_eu.groupby(pd.cut(df_eu['FIREDATE'], range_2020['bins'], right=0, include_lowest=1))['AREA_HA'].sum())
    df_count = pd.DataFrame(
        df_eu.groupby(pd.cut(df_eu['FIREDATE'], range_2020['bins'], right=0, include_lowest=1))['AREA_HA'].count())
    df_cumulative = df_area.merge(df_count, how='inner', on='FIREDATE')
    df_cumulative.index.name = 'Period'
    df_cumulative.columns = ['Area_HA', 'Count']
    # df_cumulative.to_csv('slide5.csv')
    dat = pd.DataFrame(dates_tag)
    dat.columns = ['Dates_tag']
    df_cumulative.insert(2, "Single dates", dates_tag[1:], True)
    df_cumulative = df_cumulative.reset_index()
    df_cumulative = df_cumulative.set_index('Single dates')
    print('Now plotting 2020 burnt area in EU countries')
    title = '2020 burnt area in EU countries'
    png_name = '6a_weekly_evolution.png'
    labeled_graphs(path, df_cumulative, title, png_name)

    # Burnt areas weekly evolution in non EU during the year
    df_area = pd.DataFrame(
        df_eunon_meaf.groupby(pd.cut(df_eunon_meaf['FIREDATE'], range_2020['bins'], right=0, include_lowest=1))[
            'AREA_HA'].sum())
    df_count = pd.DataFrame(
        df_eunon_meaf.groupby(pd.cut(df_eunon_meaf['FIREDATE'], range_2020['bins'], right=0, include_lowest=1))[
            'AREA_HA'].count())
    df_cumulative = df_area.merge(df_count, how='inner', on='FIREDATE')
    df_cumulative.index.name = 'Period'
    df_cumulative.columns = ['Area_HA', 'Count']
    # df_cumulative.to_csv('slide6.csv')
    df_cumulative.insert(2, "Single dates", dates_tag[1:], True)
    df_cumulative = df_cumulative.reset_index()
    df_cumulative = df_cumulative.set_index('Single dates')
    # plotting
    print('Now plotting 2020 burnt area in non EU countries')
    plt.clf()
    title = '2020 burnt area in non EU countries'
    png_name = '6b_weekly_evolution.png'
    # print(df_cumulative)
    labeled_graphs(path, df_cumulative, title, png_name)

    # Cumulative burnt area in EU, weekly evolution and comparison with historical data
    df_history_eu = df_history.loc[df_history['EU_nonEU'] == 'EU']
    df_area_2020 = pd.DataFrame(
        df_eu.groupby(pd.cut(df_eu['FIREDATE'], range_2020['bins'], right=0, include_lowest=1))[
            'AREA_HA'].sum().cumsum())
    df_area_2020.index.name = 'Period'
    df_area_2020.columns = ['Area_HA']
    hist_stats = {}
    for year in years:
        range_dates = pd.DataFrame(range.get('range_dates_{}'.format(year)))
        range_dates.columns = ['bins']
        df_year_temp = df_history_eu.loc[df_history_eu['YearSeason'] == year]
        df_year = \
            df_year_temp.groupby(pd.cut(df_year_temp['FireDate'], range_dates['bins'], right=0, include_lowest=1))[
                'Area_HA'].sum().cumsum()
        df_year.index.name = 'Period'
        df_year.columns = ['Area_HA']
        hist_stats["{}".format(year)] = df_year
    del hist_stats['2020']
    df_hist = df_area_2020.copy()
    for i in hist_stats.keys():
        df_hist[str(i)] = hist_stats.get(i).values
    df_hist = df_hist.drop('Area_HA', axis=1)
    df_hist['hist_average_from_2008'] = df_hist.mean(numeric_only=True, axis=1).round(0)
    df_hist['hist_average_from_2008'] = df_hist['hist_average_from_2008'].astype(int)
    df_area_2020 = df_area_2020.merge(df_hist['hist_average_from_2008'], how='inner', left_on='Period',
                                      right_on='Period')
    # df_area_2020.to_csv('slide7.csv')
    # plotting
    df_area_2020.insert(2, "Single dates", dates_tag[:-1], True)
    df_area_2020 = df_area_2020.reset_index()
    df_area_2020 = df_area_2020.set_index('Single dates')
    print('Now plotting Cumulative burnt area in EU countries')
    plt.clf()
    df_area_2020 = df_area_2020.rename(
        columns={"Area_HA": "2020 burnt area", "hist_average_from_2008": "2008-2019 average burnt area"})
    df_area_2020 = df_area_2020.truncate(after=(dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
    # print(df_area_2020)
    title = 'Cumulative burnt area in EU countries'
    png_name = '6c_weekly_evolution.png'
    ylabel = 'Area (ha)'
    unlabeled_graphs(path, df_area_2020, title, png_name, ylabel)

    # Cumulative fire count in EU, weekly evolution and comparison with historical data
    df_count_2020 = pd.DataFrame(
        df_eu.groupby(pd.cut(df_eu['FIREDATE'], range_2020['bins'], right=0, include_lowest=1))[
            'AREA_HA'].count().cumsum())
    df_count_2020.index.name = 'Period'
    df_count_2020.columns = ['count']
    hist_stats = {}
    for year in years:
        range_dates = pd.DataFrame(range.get('range_dates_{}'.format(year)))
        range_dates.columns = ['bins']
        df_year_temp = df_history_eu.loc[df_history_eu['YearSeason'] == year]
        df_year = \
        df_year_temp.groupby(pd.cut(df_year_temp['FireDate'], range_dates['bins'], right=0, include_lowest=1))[
            'Area_HA'].count().cumsum()
        df_year.index.name = 'Period'
        df_year.columns = ['count']
        hist_stats["{}".format(year)] = df_year
    del hist_stats['2020']
    df_hist = df_count_2020.copy()
    for i in hist_stats.keys():
        df_hist[str(i)] = hist_stats.get(i).values
    df_hist = df_hist.drop('count', axis=1)
    df_hist['hist_average_from_2008'] = df_hist.mean(numeric_only=True, axis=1).round(0)
    df_hist['hist_average_from_2008'] = df_hist['hist_average_from_2008'].astype(int)
    df_count_2020 = df_count_2020.merge(df_hist['hist_average_from_2008'], how='inner', left_on='Period',
                                        right_on='Period')
    # df_count_2020.to_csv('slide8.csv')
    # plotting
    df_count_2020.insert(2, "Single dates", dates_tag[:-1], True)
    df_count_2020 = df_count_2020.reset_index()
    df_count_2020 = df_count_2020.set_index('Single dates')
    print('Now plotting Cumulative number of fires in EU countries')
    plt.clf()
    df_count_2020 = df_count_2020.rename(
        columns={"count": "2020 burnt area", "hist_average_from_2008": "2008-2019 average burnt area"})
    df_count_2020 = df_count_2020.truncate(after=(dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
    # print(df_count_2020)
    title = 'Cumulative number of fires in EU countries'
    png_name = '6d_weekly_evolution.png'
    ylabel = 'Fires count'
    unlabeled_graphs(path, df_count_2020, title, png_name, ylabel)