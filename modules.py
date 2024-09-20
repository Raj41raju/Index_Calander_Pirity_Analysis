import pandas as pd
import  numpy as np

def pirity_calculation(all_data, close, opt_type):

    all_data['datetime'] = all_data['date'] + " " + all_data['time']
    all_data['strike_price'] = pd.to_numeric(all_data['strike_price'], errors='coerce')
    all_data['exp_date'] = pd.to_datetime(all_data['exp_date'], format="%d-%b-%Y", errors="coerce")

    all_data['date'] = pd.to_datetime(all_data['date'], format="%d-%m-%Y", errors="coerce")
    # all_data.head()
    # all_data.info()

    all_data.instrument.unique()
    start_time = "09:15:00"
    end_time = "15:29:00"
    index_data = all_data[(all_data['instrument']=='INDEX') & (all_data['time'] >=start_time) & (all_data['time']<=end_time)].copy()

    pirity_for = close
    index_close = index_data[['symbol', 'date', 'time', pirity_for, 'datetime']].copy()
    index_close.set_index('datetime', drop=True, inplace=True)
    index_close.sort_index(inplace=True)

    index_close['strike_price'] = round(index_close['close']/50)*50
    # index_close


    ##### Finding Current Expiry and Next Expiry Date
    curr_date = all_data['date'].iloc[0]
    exp_list = list(all_data.exp_date.unique())
    exp_list.sort()
    if curr_date == exp_list[0]:
        curr_exp = exp_list[1]
        nxt_exp = exp_list[2]
    else:
        curr_exp = exp_list[0]
        nxt_exp = exp_list[1]

########################### Calculating current and next Straddle ###################################
    curr_ce = all_data[(all_data['exp_date'] == curr_exp) & (all_data['option_type'] == 'CE')].copy()
    curr_pe = all_data[(all_data['exp_date'] == curr_exp) & (all_data['option_type'] == 'PE')].copy()

    nxt_ce = all_data[(all_data['exp_date'] == nxt_exp) & (all_data['option_type'] == 'CE')].copy()
    nxt_pe = all_data[(all_data['exp_date'] == nxt_exp) & (all_data['option_type'] == 'PE')].copy()

    curr_strdd_close = pd.merge(index_close, curr_ce[['date', 'time', 'strike_price', 'close']],
                                on=['date', 'time', 'strike_price'], 
                                suffixes=('', '_ce')).merge(curr_pe[['date', 'time', 'strike_price', 'close']], 
                                                            on=['date', 'time', 'strike_price'], suffixes=('', '_pe'))

    nxt_strdd_close = pd.merge(index_close, nxt_ce[['date', 'time', 'strike_price', 'close']],
                                on=['date', 'time', 'strike_price'], 
                                suffixes=('', '_ce')).merge(nxt_pe[['date', 'time', 'strike_price', 'close']], 
                                                            on=['date', 'time', 'strike_price'], suffixes=('', '_pe'))
    #Calculating current current and next straddle
    curr_strdd_close['curr_strdd'] = curr_strdd_close['close_ce'] + curr_strdd_close['close_pe']
    nxt_strdd_close['nxt_strdd'] = nxt_strdd_close['close_ce'] + nxt_strdd_close['close_pe']
    #Combining Current and next straddle
    curr_nxt_strdd = pd.merge(curr_strdd_close[['symbol', 'date', 'time', 'close', 'strike_price', 'close_ce', 'close_pe' ,'curr_strdd']],
                                nxt_strdd_close[['symbol', 'date', 'time', 'close', 'strike_price', 'close_ce', 'close_pe', 'nxt_strdd']], 
                                on=['symbol', 'date', 'time', 'close', 'strike_price'],
                                suffixes=('_curr', '_nxt'))
    #Changing datetime format to match datetime format of cal_curr_piv and  cal_nxt_piv
    curr_nxt_strdd['datetime'] = curr_nxt_strdd['date'].dt.strftime('%d-%m-%Y') +  ' ' + curr_nxt_strdd['time']
    curr_nxt_strdd.set_index('datetime', inplace=True)
    ########################## Straddle calculation done  ##########################################


    #required call strike for calander pirity calculation
    # taking only 6 ITM Strikes and 8 OTM strikes
    if opt_type =='CE':

        low_strike = int(round(index_data.low.min()/100)*100)
        high_strike = int(round(index_data.high.max()/100)*100)
        # atm_strike = list(range(low_strike, high_strike + 50, 50))
        ce_strike = list(range(low_strike - 6*50, high_strike +8*50, 50)) #Calendar strike

        curr_ce_req = curr_ce[curr_ce['strike_price'].isin(ce_strike)].copy()
        nxt_ce_req = nxt_ce[nxt_ce['strike_price'].isin(ce_strike)].copy()

        curr_ce_req.set_index('datetime', inplace=True)
        nxt_ce_req.set_index('datetime', inplace=True)

        curr_ce_req.sort_index(inplace=True)
        nxt_ce_req.sort_index(inplace=True)
        curr_ce_req['strike_price'] = curr_ce_req['strike_price'].astype('int')
        nxt_ce_req['strike_price'] = nxt_ce_req['strike_price'].astype('int')

        #Here pirity for could be open, high, low, close
        curr_ce_piv = curr_ce_req.pivot_table(index='datetime', values=pirity_for, columns='strike_price')
        nxt_ce_piv = nxt_ce_req.pivot_table(index='datetime', values=pirity_for, columns='strike_price')

        ce_calander = pd.merge(curr_ce_piv, nxt_ce_piv, left_index=True, right_index=True, how='inner', suffixes=('_curr', '_nxt'))
        # len(ce_strike)
        strike_count = len(ce_strike)
        for i, strike in enumerate(ce_strike):
            column_name = str(strike)
            ce_calander[column_name] = ce_calander.iloc[:, strike_count+i] - ce_calander.iloc[:, i]
            # break

    #Extracting only calander pirity
    ce_calander_pirity = ce_calander.iloc[:, 36:].copy()

    #This will incluse current straddle, next straddle, index details and 
    #indivisual ce and pe ATM stricks for current and next expiry
    ce_calander_pirity_details = pd.merge(curr_nxt_strdd, ce_calander_pirity, left_index=True, right_index=True, how='inner')
    

    #ANALYSING CALANDER PIRITY DATA
    cal_summ = ce_calander_pirity.describe()
    # cal_summ.reset_index(inplace=True)
    # # data_summ.drop(columns='strike_price', inplace=True)
    # cal_summ.rename(columns= {'index':'summ'}, inplace=True)
    cal_summ.columns.name=None

    cal_summ_transpos = cal_summ.transpose()
    cal_summ_transpos.reset_index(inplace=True)
    cal_summ_transpos.rename(columns={'index':'strike'})
    cal_summ_transpos.columns = pd.MultiIndex.from_tuples([('strike', '')] + [('close', col) for col in cal_summ_transpos.columns[1:]])
    cal_summ_transpos.drop(columns=('close', 'count'), inplace=True)
    # Insert a column name "date" at 0th index
    cal_summ_transpos.insert(0, 'date', curr_date.date())
    return curr_nxt_strdd, ce_calander_pirity_details, cal_summ_transpos