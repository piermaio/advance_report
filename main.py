### Advance report for Tracy

import os
import db_connection
import ba_and_number_by_country
import monthly_ba_and_number
import ba_by_fire_class_in_eu
import list_eu_fires_gt_500ha
import weekly_evolutions
import landcover_by_country_and_comparison_with_history
import corine_landuse_stats_by_country
import natura2000_corinelandcover_stats_by_country


def main():
    root = 'C:/Users/piermaio/Documents/gisdata/jrc/advance_report/'
    corine_path = 'C:\\Users\\piermaio\\Documents\\gisdata\\jrc\\BAmapping\\Corine\\raster\\Corine_globcover_MA_TU_ukraine.tif'  # PM local path to the corine geotif
    if not os.path.exists(root):
        os.makedirs(root)
    # defining the table from which the data are retrieved and connecting
    table_name = 'gw_burntarea_effis.ba_oracle_export_year'
    df_sql, gdf_sql, nat2k_year, nat2kweek, df_nations, nat2k_by_country, nat2k_areas = db_connection.db_connection(table_name)
    ba_ref = ba_and_number_by_country.sum_count(root, df_sql, df_nations)
    # monthly_ba_and_number.month_sum_count(root, df_sql)
    # ba_by_fire_class_in_eu.sum_count(root, df_sql)
    # list_eu_fires_gt_500ha.sum_count(root, df_sql, df_nations)
    # weekly_evolutions.sum_count(root, df_sql, df_nations)
    # df_tab_corine = corine_landuse_stats_by_country.corine_stats(root, ba_ref, gdf_sql, corine_path)
    # PM restore after developing chapter 7
    # landcover_by_country_and_comparison_with_history.corine_stats(root, df_tab_corine)
    # landcover_by_country_and_comparison_with_history.corine_stats(root)
    natura2000_corinelandcover_stats_by_country.main()
    # PM
    return 0



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# check 2,
