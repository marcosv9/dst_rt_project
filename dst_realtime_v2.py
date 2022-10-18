
import pandas as pd
import requests
import datetime
import numpy as np
from pandas.tseries.frequencies import to_offset
from calendar import monthrange
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import os
import pathlib

def get_realtime_dst(starttime,
                     endtime):
    '''
    
    '''
    #validating input format
    for i in [starttime, endtime]:
        validate_date_input(i)
           
    
    working_directory = os.getcwd()
    
    pathlib.Path(os.path.join(working_directory, 'dst_yearly_files')).mkdir(parents = True, exist_ok = True) 
    #dates = pd.date_range(np.datetime64(starttime),
    #                      np.datetime64(endtime) +1,
    #                      freq = 'M')
    
    today = datetime.datetime.today()
    
    datem = datetime.datetime(today.year, today.month, 1)
    
    df_final = pd.DataFrame()
    
    df_definitive = pd.read_html('https://wdc.kugi.kyoto-u.ac.jp/dst_final/index.html')
    
    #df_provisional = pd.read_html('https://wdc.kugi.kyoto-u.ac.jp/dst_provisional/index.html')
    
    df_current = pd.read_html('https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/index.html')
    
    
    #getting dates that we already heve a dst file in the database 
    
    dates_checked = check_dst_in_database(starttime,
                                          endtime)
    
    
    dates = []
    
    for i in dates_checked:
        dates += [pd.to_datetime(i)]
        
    #cheking best available datatype for the period, if exists, file will be replaced

    dates += check_best_available_datatype(starttime, endtime)
    
    
    #cheking if endtime is greater than current month, this way current month will always be updated
    if pd.to_datetime(endtime) >= datem:
        dates += [pd.to_datetime(datem)]
    
    #avoiding duplicate dates    
    dates = list(set(dates))  
    
    for date in dates:
        if pd.to_datetime(date) > datem:
            dates.remove(date)
    
    #sorting dates  
    dates.sort()
    
    if len(dates) != 0:    
        for date in dates:
        
        
        #cheking if data must be downloaded from final, provisional or real-time
        
            if int(date.year) <= int(df_definitive[0].Year.values[-1]):
                
                datatype = 'F'
                html = f'https://wdc.kugi.kyoto-u.ac.jp/dst_final/{date.year}{str(date.month).zfill(2)}/index.html'   
                url = requests.get(html).text
                
            elif int(date.year) > int(df_definitive[0].Year.values[-1]) and int(date.year) < int(df_current[0].Year.values[0]):
                
                datatype = 'P'
                html = f'https://wdc.kugi.kyoto-u.ac.jp/dst_provisional/{date.year}{str(date.month).zfill(2)}/index.html'   
                url = requests.get(html).text
                
            else:
                datatype = 'RT'
                html = f'https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{date.year}{str(date.month).zfill(2)}/index.html'   
                url = requests.get(html).text                
                
        
            soup = BeautifulSoup(url, "html.parser")
            #soup = BeautifulSoup(url,"lxml")
            data = soup.find('pre', class_='data').get_text()
            days_in_month = monthrange(date.year, date.month)[1]
            
            with open(r'dst_'+ str(date.year) + '_' + str(date.month).zfill(2) + '_realtime.txt', 'w') as fp:
                for item in data:
            # write each item on a new line
                    fp.write(item)
        
        #download if date is the current month            
            if pd.to_datetime(f'{date.year}-{date.month}-{date.day}') >= datem:
                
                today = datetime.datetime.today().date()
                days_in_month = monthrange(today.year, today.month)[1]
                
                #reading tmp dst file to sctructure the data
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
                    
                daily_index = []
                values = []
                
                for i,j in zip(df.index, range(len(df.index))):
                    values.extend(df.iloc[j].values)
                    for col in df.columns:
                        daily_index.append(i + to_offset(f'{df[col].name}H'))
                        
                df_dst_current = pd.DataFrame()
                df_dst_current.index = daily_index
                df_dst_current.index.name = 'Date'
                df_dst_current['Values'] = values
                df_dst_current['datatype'] = datatype
                df_dst_current = df_dst_current.shift(-1, freq = 'H')
                df_dst_current['Values'] = df_dst_current['Values'].replace(9999.0, np.nan)
                
        
        #download for all months that are not current month                      
            else:
                        
                pd.read_csv(f'dst_{date.year}_{str(date.month).zfill(2)}_realtime.txt',
                            skiprows = 8,
                            header = None,
                            sep = ',').to_csv(f'dst_{date.year}_{str(date.month).zfill(2)}_realtime.txt',
                                              sep = '\t',
                                              header = None,
                                              index = False)
                
                with open(f'dst_{date.year}_{str(date.month).zfill(2)}_realtime.txt') as f:
                    lines = f.readlines()
                 
                n = 4
                comp_day = [] #list with dst values for the day
                for line in range(len(lines)):
                    #removing additional spaces from lines
                    lines[line] = lines[line][3:35] + lines[line][36:68] + lines[line][69:101]
                    
                    comp_day += [lines[line][i:i+n] for i in range(0, len(lines[line]), n)] 
                    
                    
                time_index = pd.date_range(f'{date.year}-{date.month}',
                                         f'{date.year}-{date.month}-{days_in_month} 23:00:00',
                                         freq = 'H'
                                         )
                df_dst = pd.DataFrame(index = time_index, data = comp_day)   
                 
                #df = df.replace('99999999999999999999999999999999',
                #                np.nan)
                
                #date = pd.to_datetime(datetime.datetime.date().date(), format = '%Y-%m-%d')
                #df = df.replace(df.loc[str(date)][df.loc[str(date)].last_valid_index()], np.nan)
                    
                #for col in df.columns:
                #    df[col] = pd.to_numeric(df[col])
                df_dst[0] = pd.to_numeric(df_dst[0])    
                #index_list = []
                #values = []
                #for i,j in zip(df.index, range(len(df.index))):
                #    values.extend(df.iloc[j].values)
                #    for col in df.columns:
                #        index_list.append(i + to_offset(f'{df[col].name}H'))
                        
                #df_dst = pd.DataFrame()
                #df_dst.index = index_list
                df_dst.index.name = 'Date'
                df_dst = df_dst.rename(columns = {0: 'Values'})
                df_dst['datatype'] = datatype
                #df_dst = df_dst.shift(-1, freq = 'H')
                
                df_final = pd.concat([df_final, df_dst])
                #print(df_final, 'df_final')
        
        #concat current month with other months        
        if datetime.datetime(date.year, date.month, date.day) >= datem:
            
            
            df_final = pd.concat([df_final, df_dst_current.dropna()])
            
            
        #creating list of years without files and no duplicates
        years = []
        for i in dates:
            years += [i.year]
            
        years = list(set(years))   
            
        for year in years:
            #if file exist, reading from database
            if os.path.isfile(os.path.join(working_directory,
                                           'dst_yearly_files',
                                           f'dst_{year}.txt')) == True:
            
                df = pd.read_csv(os.path.join(working_directory,
                                              'dst_yearly_files',
                                              f'dst_{str(year)}.txt'
                                              ),
                                 sep = '\t',
                                 index_col= [0]
                                 )
                df.index = pd.to_datetime(df.index, format = '%Y-%m-%d %H:%M:%S')
                
            
                df_final = pd.concat([df, df_final]).sort_index()
                
                df_final = df_final[~df_final.index.duplicated(keep='last')]
        
                  
            df_final.loc[str(year)].replace(9999, np.nan).dropna().to_csv(os.path.join(working_directory,
                                                                                       f'dst_yearly_files',
                                                                                       f'dst_{year}.txt'
                                                                                       ),
                                                                              sep = '\t',
                                                                              encoding='ascii')
        #deleting tmp files
        for date in dates:
            if os.path.isfile(os.path.join(working_directory,
                                           f'dst_{date.year}_{str(date.month).zfill(2)}_realtime.txt'
                                           )
                              ) == True:
                
                os.remove(f'dst_{date.year}_{str(date.month).zfill(2)}_realtime.txt')
            
        

    dates = pd.date_range(str(starttime),
                      str(endtime),
                      freq = 'M'
                      )
    for year in dates.year.drop_duplicates():
        
        df = pd.read_csv(os.path.join(working_directory,
                                      'dst_yearly_files',
                                      f'dst_{str(year)}.txt'
                                      ),
                         sep = '\t',
                         index_col= [0]
                         )
        df.index = pd.to_datetime(df.index, format = '%Y-%m-%d %H:%M:%S')
        
        df_final = pd.concat([df, df_final]).sort_index()
        df_final = df_final[~df_final.index.duplicated(keep='last')]
             
    
    return df_final.loc[starttime:endtime]

def check_dst_in_database(starttime,
                          endtime):
    
    '''
    check existence of dst files in database
    
    for the selected period
    
    return dates where we don't have data in database
    '''
    
    for i in [starttime, endtime]:
        validate_date_input(i)
        
    working_directory = os.getcwd()
    
    years_without_files = []
    
    
    time_interval = pd.date_range(starttime,
                                  f'{endtime} 23:00:00' ,
                                  freq = 'H')
    df_dst = pd.DataFrame()
    
    for years in time_interval.year.drop_duplicates():
        
        if os.path.isfile(os.path.join(working_directory,
                                       'dst_yearly_files',
                                       f'dst_{years}.txt')) == True:
            
            df = pd.read_csv(os.path.join(working_directory,
                                          'dst_yearly_files',f'dst_{str(years)}.txt'
                                          ),
                             sep = '\t',
                             index_col= [0]
                             )
            df.index = pd.to_datetime(df.index, format = '%Y-%m-%d %H:%M:%S')
            
            df_dst = pd.concat([df_dst, df])
        else:
            years_without_files += [years]
            dates_without_files = []
            for year in years_without_files:
                for month in range(1,13):
                    dates_without_files += [f'{year}-{str(month).zfill(2)}']       
    #dates_not_in_database = []
    missing_date = []
    if not df_dst.empty:  
        for date in time_interval:
            #print(date)     
            if date not in df_dst.index:
                #dates_not_in_database += [str(date)]
                missing_date += [f'{str(date.year).zfill(2)}-{str(date.month).zfill(2)}']
                missing_date = list(set(missing_date))
        missing_date += years_without_files
    else:
        missing_date = dates_without_files
    #    for year in years_without_files:
    #        for month in range(1,13):
    #            
    #            missing_date = years_without_files
    
    #missing_date.sort()
      
    #df_dst = df_dst.resample('M').mean()    
    return missing_date 
      
def update_datatype_periods():
    '''
    Function to updated start e end data of each
    datatype period
    
    '''
     
    working_directory = os.getcwd()
    
       
    df_datatype = pd.DataFrame()
    
    df_final = pd.read_html('https://wdc.kugi.kyoto-u.ac.jp/dst_final/index.html')
    
    df_final[0].index = df_final[0]['Year']
    df_final[0].pop('Year')
    df_final[0].drop(['Unnamed: 1','Unnamed: 14','Unnamed: 15',
                      'Unnamed: 16','Unnamed: 17','Unnamed: 18',
                      'Unnamed: 19','Unnamed: 20','Unnamed: 21'
                      ],
                       axis=1, inplace=True
                       )
    final_last_date = f'{df_final[0].index[-1]}-{str(df_final[0].iloc[-1].max()).zfill(2)}'
    final_first_date = f'{df_final[0].index[0]}-{str(df_final[0].iloc[0].min()).zfill(2)}'
    
    df_provi = pd.read_html('https://wdc.kugi.kyoto-u.ac.jp/dst_provisional/index.html')
    
    df_provi[0].index = df_provi[0]['Year']
    df_provi[0].pop('Year')
    df_provi[0].drop(['Unnamed: 1','Unnamed: 14','Unnamed: 15',
                      'Unnamed: 16','Unnamed: 17','Unnamed: 18',
                      'Unnamed: 19','Unnamed: 20','Unnamed: 21'
                      ],
                       axis=1, inplace=True
                       )
    provi_last_date = f'{df_provi[0].index[-1]}-{str(df_provi[0].iloc[-1].max()).zfill(2)}'
    provi_first_date = f'{df_provi[0].index[0]}-{str(df_provi[0].iloc[0].min()).zfill(2)}'
    
    df_datatype['datatype'] = ['F','P']
    df_datatype['starttime'] = [final_first_date, provi_first_date]
    df_datatype['endtime'] = [final_last_date, provi_last_date]        
    
    df_datatype.to_csv(os.path.join(working_directory,
                                    'dst_yearly_files',
                                    'datatype_intervals.txt'),
                       sep = ',',
                       index = False)
    return

def check_best_available_datatype(starttime,
                                  endtime):
    
    '''
    function to check existence of best datatype
    availability for the selected period
    '''
    
    for i in [starttime, endtime]:
        validate_date_input(i)
        
    working_directory = os.getcwd()
       
    update_datatype_periods()
    
    date_interval = pd.date_range(starttime, endtime, freq = 'M') 
    
    df_final = pd.DataFrame()
    
    wrong_dt_dates = []
    
    for year in date_interval.year.drop_duplicates():
                
        if os.path.isfile(os.path.join(working_directory,
                                       'dst_yearly_files',
                                       f'dst_{year}.txt'
                                       )
                          ) == True:
        
            df = pd.read_csv(os.path.join(working_directory,
                                          'dst_yearly_files',
                                          f'dst_{str(year)}.txt'
                                          ),
                             sep = '\t',
                             index_col= [0]
                             )
            df.index = pd.to_datetime(df.index, format = '%Y-%m-%d %H:%M:%S')
        
            df_final = pd.concat([df, df_final]).sort_index()
            #df_final = df_final[~df_final.index.duplicated(keep='first')]
            
    if df_final.empty == True:
        
        return []
    else:
        
        df_datatype = pd.read_csv(os.path.join(working_directory,
                                               'dst_yearly_files',
                                               'datatype_intervals.txt'),
                                  sep = ',',
                                  index_col = 'datatype'
                                  )
        if pd.to_datetime(starttime) < pd.to_datetime(df_datatype['endtime'][0]):
            datatype = 'F'
            df2 = df_final.loc[starttime:str(pd.to_datetime(df_datatype['endtime'][0]).date())]
            #df2.loc[df2['datatype'] != datatype].index
            
            for i in (df2.loc[df2['datatype'] != datatype].index).date:
                wrong_dt_dates += [f'{i.year}-{str(i.month).zfill(2)}']
                wrong_dt_dates = list(set(wrong_dt_dates))
                wrong_dt_dates.sort()
            
        if (pd.to_datetime(endtime) > pd.to_datetime(df_datatype['endtime'][0])):
            datatype = 'P'
            df2 = df_final.loc[str(pd.to_datetime(df_datatype['starttime'][1]).date()):str(pd.to_datetime(df_datatype['endtime'][1]).date())]

            for i in (df2.loc[df2['datatype'] != datatype].index).date:
                
                wrong_dt_dates += [f'{i.year}-{str(i.month).zfill(2)}']
                wrong_dt_dates = list(set(wrong_dt_dates))
                wrong_dt_dates.sort() 
        
        wrong_dt_dates2 = []
        for i in wrong_dt_dates:
            wrong_dt_dates2 += [pd.to_datetime(i)]
        
        wrong_dt_dates = wrong_dt_dates2
                 
        for i in wrong_dt_dates.copy():
            
            if i not in pd.date_range(starttime, endtime, freq = 'D'):
                
                wrong_dt_dates.remove(i)          
        #elif (pd.to_datetime(endtime) > pd.to_datetime(df_datatype['endtime'][1])):
        #    datatype = 'RT'
        #
        #df2 = df_final.loc[starttime:endtime]
        #df2.loc[df2['datatype'] != datatype].index
    
    return wrong_dt_dates

def plot_dst_index(starttime,
                   endtime):
    
    """
    Function to plot the dst index for a given interval
    
    Only consider data in the dst database
    
    Use get_realtime_dst function if you don't have a database yet 
    """
    
    for i in [starttime, endtime]:
        validate_date_input(i)
    
    time_interval = pd.date_range(starttime,
                                  f'{endtime} 23:00:00' ,
                                  freq = 'H')
    
    working_directory = os.getcwd()
    
    df_dst = pd.DataFrame()
    
    for years in time_interval.year.drop_duplicates():
        
        if os.path.isfile(os.path.join(working_directory,
                                       'dst_yearly_files',
                                       f'dst_{years}.txt')) == True:
            
            df = pd.read_csv(os.path.join(working_directory,
                                          'dst_yearly_files',f'dst_{str(years)}.txt'
                                          ),
                             sep = '\t',
                             index_col= [0]
                             )
            df.index = pd.to_datetime(df.index, format = '%Y-%m-%d %H:%M:%S')
            
            df_dst = pd.concat([df_dst, df])
    
    if df_dst.empty is True:
       print('No dst data in the database for the selected period!')
          
    else:
        df_dst = df_dst.loc[starttime:endtime]
        
        plt.figure(figsize=(12,4))
        plt.title('Dst index')
        plt.xlim(df_dst.index[0], df_dst.index[-1])
        plt.plot(df_dst.Values)
        plt.xlabel('Date')
        plt.axhline(-50, ls =  '--', color ='black')
        plt.grid(alpha = 0.3)
        plt.ylabel('nT')  
        plt.show()        
  
def validate_date_input(str_date):
    """
    Function to validate input format "YYYY-MM-DD"
    
    """
    try:
        datetime.datetime.strptime(str_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError('Incorrect date format, should be YYYY-MM-DD')
 
  
if __name__ == '__main__':
    
    df = get_realtime_dst(starttime= '1957-01-01',
                          endtime = '2022-10-18')
    print(df)    
    #dates = check_dst_in_database(starttime = '2020-01-01',
    #                           endtime = '2022-10-30')
    #print(dates)

    #update_datatype_periods()
    
    #df = check_best_available_datatype('2021-06-01','2022-12-31')
    #print(df)
    
    #plot_dst_index(starttime = '1957-10-15', endtime = '2022-10-18')