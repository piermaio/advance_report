### Advance report for Tracy

import os
import ba_and_number_by_country
import monthly_ba_and_number
import ba_by_fire_class_in_eu
import list_eu_fires_gt_500ha

def main():
    root = 'C:/Users/piermaio/Documents/gisdata/jrc/advance_report/'
    if not os.path.exists(root):
        os.makedirs(root)
    # ba_and_number_by_country.sum_count(root)
    # monthly_ba_and_number.month_sum_count(root)
    # ba_by_fire_class_in_eu.sum_count(root)
    list_eu_fires_gt_500ha.sum_count(root)
    return 0



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
