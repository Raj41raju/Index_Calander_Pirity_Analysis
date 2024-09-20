import pandas as pd
import numpy as np
import os
import glob
from modules import pirity_calculation

data_path = "E:\\Finwesiya_Data_Historical\\Nifty"
#### Read All Data Files
files_location = glob.glob(os.path.join(data_path, "*.csv"))
location = pd.DataFrame(files_location, columns=['location'])

location['date'] = location['location'].apply(lambda  x: x.split('_')[-1].split('.')[0])
location['date'] = pd.to_datetime(location['date'], format = "%d-%m-%Y")
location.sort_values(by='date', inplace=True)
location.reset_index(drop=True, inplace=True)

#### FInding ATM for Whole Day and Relevent Strikes
##### For Calender we will take 6 ITM Strikes and 8 OTM Strikes

final_strdd = pd.DataFrame()
final_calender_summ = pd.DataFrame()
for i, file in enumerate(location.location[:]):
    curr_date = location['date'].iloc[i].date()
    # print(curr_date)
    # break
    data = pd.read_csv(file)
    all_data = data.copy()
    strdd,strdd_ce_pirity,cal_summary = pirity_calculation(all_data, 'close', 'CE')
    
    '''
    we can append data into straddle and summery table not in detailed calander parity dataframe
    '''

    #daily details strddale and calander price and pirity
    filename = "E:\\calander\\CE_Calender_Daily\\" + "Nifty_CE_Calendar_" + str(curr_date) +  ".csv"
    # print(filename)
    strdd_ce_pirity.to_csv(filename,  index=False)


    # print(a.head())
    # print(b.head())
    # print(c.head())

    final_strdd = pd.concat([final_strdd, strdd], axis=0, ignore_index=True)
    print(len(final_strdd))

    final_calender_summ = pd.concat([final_calender_summ, cal_summary], axis=0, ignore_index=True)

    # break
strdd_filename = "E:\\calander\\" + "Nifty_Strdd_" + str(curr_date) +  ".csv"
final_strdd.to_csv(strdd_filename,  index=False)

cal_summary_filename = "E:\\calander\\" + "Nifty_CE_calander_summary_" + str(curr_date) +  ".csv"
final_calender_summ.to_csv(cal_summary_filename, index=False)
