import pandas as pd
import urllib.request
import requests
import datetime
import numpy as np
from pandas.tseries.frequencies import to_offset
from calendar import monthrange
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import os


def get_realtime_dst(save_files:bool = False,
                     save_plots:bool = False):
    
    '''
    Function to get realtime dst index
    
    Data provided by WDC for Geomagnetism, Kyoto 
    
    https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/presentmonth/index.html
    
    The function access the webpage for realtime dst index
    and get the information of previous and next month
    
    -------------------------------------------------------------------------
    Arguments:
    
    save_files: boolean - True or False to save a txt file with the data
                          One file per month
                          
    save_plots: boolean - True or False to save the plots
    --------------------------------------------------------------------------------
    
    Return a pandas dataframe of the realtime dst index
    
    '''

    #asserting if inputs are boolean (True or False)
    
    assert isinstance(save_files, (bool)), 'save_files must be True or False'
    
    assert isinstance(save_plots, (bool)), 'save_plots must be True or False'
    
    current_month = requests.get('https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/presentmonth/index.html').text

    last_month = requests.get('https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/lastmonth/index.html').text
    
    
    #getting dst data for current month
    url = current_month
    today = datetime.datetime.today().date()
    days_in_month = monthrange(today.year, today.month)[1]
    
    
    soup = BeautifulSoup(url,"html.parser")
    #soup = BeautifulSoup(url,"lxml")
    data = soup.find('pre', class_='data').get_text()
    
    with open(r'dst_'+ str(today.year) + '_' + str(today.month) + '_realtime.txt', 'w') as fp:
        for item in data:
        # write each item on a new line
            fp.write(item)
    #print(f'file dst_{today.year}_{today.month}_realtime.txt saved!')   
    
    df = pd.read_csv(f'dst_{today.year}_{today.month}_realtime.txt',
                 skiprows = 8,
                 header = None,
                 sep = '\s+',
                 index_col = [0],
                 dtype = str)
     
    df.index = pd.date_range(f'{today.year}-{today.month}',
                             f'{today.year}-{today.month}-{days_in_month}',
                             freq = 'D'
                             )
        
    df = df.replace('99999999999999999999999999999999',
                    np.nan)
    
    #today = pd.to_datetime(datetime.datetime.today().date(), format = '%Y-%m-%d')
    df = df.replace(df.loc[str(today)][df.loc[str(today)].last_valid_index()], np.nan)
        
    for col in df.columns:
        df[col] = pd.to_numeric(df[col])
        
    dates = []
    values = []
    for i,j in zip(df.index, range(len(df.index))):
        values.extend(df.iloc[j].values)
        for col in df.columns:
            dates.append(i + to_offset(f'{df[col].name}H'))
            
    df_dst_current = pd.DataFrame()
    df_dst_current.index = dates
    df_dst_current.index.name = 'Date'
    df_dst_current['value'] = values
    df_dst_current = df_dst_current.shift(-1, freq = 'H')
    
    #getting dst for the last month
    
    url = last_month
    lastmonth_date = datetime.datetime.today().date() + to_offset('-1M')
    days_in_last_month = monthrange(lastmonth_date.year, lastmonth_date.month)[1]
    
    soup = BeautifulSoup(url,'html.parser')
    data = soup.find('pre', class_='data').get_text()
    
    with open(r'dst_'+ str(lastmonth_date.year) + '_' + str(lastmonth_date.month) + '_lastmonth.txt', 'w') as fp:
        for item in data:
        # write each item on a new line
            fp.write(item)
    
    df = pd.read_csv(f'dst_{lastmonth_date.year}_{lastmonth_date.month}_lastmonth.txt',
                     skiprows = 8,
                     header = None,
                     sep = '\s+',
                     index_col = [0],
                     dtype = str) 
    
    df.index = pd.date_range(f'{lastmonth_date.year}-{lastmonth_date.month}',
                             f'{lastmonth_date.year}-{lastmonth_date.month}-{days_in_last_month}',
                             freq = 'D') 
    for col in df.columns:
        df[col] = pd.to_numeric(df[col])
        
    dates = []
    values = []
    for i,j in zip(df.index, range(len(df.index))):
        values.extend(df.iloc[j].values)
        for col in df.columns:
            dates.append(i + to_offset(f'{df[col].name}H'))
            
    df_dst_last = pd.DataFrame()
    df_dst_last.index = dates
    df_dst_last.index.name = 'Date'
    df_dst_last['value'] = values
    df_dst_last = df_dst_last.shift(-1, freq = 'H')
     
    if save_files == True:
        df_dst_current.to_csv(f'dst_{today.year}_{today.month}_realtime.txt',
                              sep = '\t')
        df_dst_last.to_csv(f'dst_{lastmonth_date.year}_{lastmonth_date.month}_lastmonth.txt',
                           sep = '\t')
        
        print(f'files saved -> {os.getcwd()}!')
    else:
        os.remove(f'dst_{today.year}_{today.month}_realtime.txt')
        os.remove(f'dst_{lastmonth_date.year}_{lastmonth_date.month}_lastmonth.txt')
        
    #plotting figure
    
    df_total = pd.concat([df_dst_last, df_dst_current])
    df_total = df_total.dropna()
    
    plt.figure(figsize = (14,4))
    
    plt.plot(df_total,
             color = 'blue',
             label = 'Last month')
    
    plt.plot(df_dst_current,
             color = 'red',
             label = 'Current month')
    
    plt.xlim(df_total.index[0], df_total.index[-1])
    plt.title('Dst Real-Time')
    plt.xlabel('Date')
    plt.ylabel('nT')
    plt.axhline(-50, ls =  '--')
    plt.grid(alpha = 0.3)
    plt.legend()
     
    if save_plots == True:
        plt.savefig(f'dst_realtime_{today.year}{today.month}{today.day}.jpg',
                    bbox_inches = 'tight',
                    dpi = 300)
        print(f'Plots saved -> {os.getcwd()}!')       
    plt.show()
    
    #plt.figure(figsize = (14,4))
    #plt.plot(df_dst_current.interpolate(method = 'spline', order = 3))
    #plt.show()
    
    return df_dst_current
if __name__ == '__main__':
    df = get_realtime_dst(save_plots = False, save_files = False)