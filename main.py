### Advance report for Tracy

import os
import pandas as pd
import db_connection
import ba_and_number_by_country
import natura2000_protected_areas
import monthly_ba_and_number
import ba_by_fire_class_in_eu
import list_eu_fires_gt_500ha
import weekly_evolutions
import landcover_by_country_and_comparison_with_history
import corine_landuse_stats_by_country
import natura2000_corinelandcover_stats_by_country
import natura2000_sites_list_by_country

global df_tab_corine

def main():
    root = 'C:/Users/piermaio/Documents/gisdata/jrc/advance_report/'
    corine_path = 'C:\\Users\\piermaio\\Documents\\gisdata\\jrc\\BAmapping\\Corine\\raster\\Corine_globcover_MA_TU_ukraine.tif'  # PM local path to the corine geotif
    if not os.path.exists(root):
        os.makedirs(root)
    os.chdir(root)
    # defining the table from which the data are retrieved and connecting
    table_name = 'gw_burntarea_effis.ba_oracle_export_year'
    df_sql, gdf_sql, nat2k_year, nat2kweek, df_nations, nat2k_by_country, nat2k_areas, df_nat2k_countries\
        , df_nat2k_sort_countries, df_nat2k_sort_area = db_connection.db_connection(table_name)

    # Chapter 1
    print('-- Processing chapter 1\n')
    ba_ref = ba_and_number_by_country.main(df_sql, df_nations)

    # Chapter 2
    print('-- Processing chapter 2\n')
    natura2000_protected_areas.main(df_nat2k_countries)

    # Chapter 3
    print('-- Processing chapter 3\n')
    monthly_ba_and_number.main(df_sql, df_nations)

    # Chapter 4
    print('-- Processing chapter 4\n')
    ba_by_fire_class_in_eu.main(df_sql, df_nations)

    # Chapter 5
    print('-- Processing chapter 5\n')
    list_eu_fires_gt_500ha.sum_count(df_sql, df_nations)

    # Chapter 6
    print('-- Processing chapter 6\n')
    weekly_evolutions.main(df_sql, df_nations)

    # Chapter 8 (PM - contains a tab necessary for chapter 7)
    print('-- Processing chapter 8\n')
    df_tab_corine = corine_landuse_stats_by_country.main(ba_ref, gdf_sql, corine_path)

    # Chapter 7 (PM - df_tab_corine from chapter 8 requested)
    print('-- Processing chapter 7\n')
    # df_tab_corine = pd.read_csv('temp_corine.csv')
    landcover_by_country_and_comparison_with_history.main(df_tab_corine)

    # Chapter 9 (PM - still under construction - see sql_queries, also it requires a new intersection between ba and nat2k followed by zonal stats over the corine)
    print('-- Processing chapter 9\n')
    print('Not available until we define the right natura2k layer used, also it requires a new intersection between ba and nat2k followed by zonal stats over the corine')
    # natura2000_corinelandcover_stats_by_country.main()


    # Chapter 10
    print('-- Processing chapter 10\n')
    natura2000_sites_list_by_country.main(df_nat2k_sort_countries, df_nat2k_sort_area)

    print('-- End of processing --\n')
    return 0


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# check 2,
