# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 09:24:41 2023

@author: vvisipkov
"""

import pandas as pd
import os

from cls_direct import CLSDirect

cls_direct_path = os.path.join("G:\\","QRS - QAG lead research","CLSDirect")

product_series = "FXSPTVL01H:DAIL" # "FXSPTOF01D:DAIL"
from_date = "2011-10-31"
to_date = "2023-02-28"

cls_direct_product_path = os.path.join(cls_direct_path, product_series.replace(":", "_"))
os.chdir(cls_direct_product_path)


currency_pairs = ['AUDCAD','AUDCHF','AUDJPY','AUDNZD','AUDUSD','CADJPY','CHFJPY','EURAUD','EURCAD','EURCHF',
               'EURDKK','EURGBP','EURHUF','EURJPY','EURNOK','EURNZD','EURSEK','EURSGD','EURUSD','GBPAUD',
               'GBPCAD','GBPCHF','GBPJPY','GBPUSD','NOKSEK','NZDJPY','NZDUSD','USDCAD','USDCHF','USDDKK',
               'USDHKD','USDHUF','USDILS','USDJPY','USDKRW','USDMXN','USDNOK','USDSEK','USDSGD','USDZAR']

#Examples on how to use the class
pd.set_option("max_columns", None)

# *** Find all meta dataset information ***
cls_direct = CLSDirect()
dataset = cls_direct.all_dataset_metadata()

#Query for dataset code
dataset.loc[(dataset['Dataset'] == "FX Spot Volume"), ["Dataset_Code", "Dataset_Name", "Update_Frequency"]]

# *** Extract data series ***
# data = cls_direct.product_series(product_series, "2021-01-01", "2021-12-31", "EURUSD")

# print(data.head())

for ccy_pair in currency_pairs:
    print(ccy_pair)
    data = cls_direct.product_series(product_series, from_date, to_date, ccy_pair)
    df = data.sort_values(['fx_business_date', 'london_date', 'hour'])
    file_name = ccy_pair + ".csv"
    df.to_csv(file_name, index=False)

# print(FXSPTVL01H_DAIL.size)
# print(FXSPTVL01H_DAIL.head())
