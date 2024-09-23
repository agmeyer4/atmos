"""Utitlities for handling meteorological data

"""

import pandas as pd
import datetime 
import numpy as np
import os 
import sys
import pytz
import re
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
from configs.met_config import MetConfig
from utils import df_utils

class MetHandler(MetConfig):
    def __init__(self,config_mode='default'):
        super().__init__(config_mode)
    
    def load_data_in_range(self,met_type,data_path,dtr=None,start_dt=None,end_dt=None,tz='UTC'):
        if dtr is None:
            if start_dt is None or end_dt is None:
                raise ValueError("Either a DateTimeRange object or both start_dt and end_dt must be provided.")
            dtr = DateTimeRange(start_dt, end_dt, tz)
        
        if met_type == 'vaisala_tph':
            vtph = VaisalaTPH(data_path)
            df = vtph.load_df_in_range(dtr)
            df = self.standardize(df)
            if df.index.tz != dtr.tz:
                df.index = df.index.tz_convert(dtr.tz)
            df = df.loc[dtr.start_dt:dtr.end_dt]
            return df

    def standardize(self,df):
        if 'dt' in df.columns:
            df.set_index('dt', inplace=True)
        elif df.index.name != 'dt':
            raise ValueError("DataFrame must have a 'dt' column or a datetime index named 'dt'.")
        extra_columns = [col for col in df.columns if col not in self.default_vars.keys()]
        if extra_columns:
            print(f"Warning: Extra columns in DataFrame that are not in default_vars: {extra_columns}")
        return df

class GGGMetHandler():
    def __init__(self,df,met_type):
        if df.index.tz != pytz.UTC:
            df.index = df.index.tz_convert(pytz.UTC)
        self.df = df
        self.met_type = met_type
        self.ggg_df = self.prep_df_for_ggg()

    def write_daily_ggg_met_files(self,write_path,overwrite=False):
        daily_dfs = [part for _, part in self.ggg_df.groupby(pd.Grouper(freq='1D')) if not part.empty] #parse into a list of daily dataframes
        for day_df in daily_dfs:
            self.write_ggg_met_file(day_df,write_path,overwrite)

    def write_ggg_met_file(self,day_df,write_path,overwrite=False):
        if day_df.index.tz != pytz.UTC:
            raise ValueError("DataFrame index must be in UTC.")
        
        unique_dates = pd.Index(day_df.index.date).unique()
        if len(unique_dates) != 1:
            raise ValueError("DataFrame index must contain data from only one day.")

        if self.met_type == 'vaisala_tph':
            file_ext = '_vtph.txt'
        else:
            file_ext = '.txt'
        
        full_fname = os.path.join(write_path,unique_dates[0].strftime('%Y%m%d')+file_ext)    
        if os.path.exists(full_fname) and not overwrite:
            raise FileExistsError(f"File already exists: {full_fname}")

        if len(day_df.dropna()) == 0:
            return

        with open(full_fname,'w') as f:
            day_df.to_csv(f,sep=',',index = False)

    def prep_df_for_ggg(self):
        ggg_column_map = {
            'pres': 'Pout',
            'temp': 'Tout',
            'rh': 'RH',
            'ws': 'WSPD',
            'wd': 'WDIR',
        }
        ggg_column_order = ['UTCDate','UTCTime','Pout','Tout','RH','WSPD','WDIR']
        df = self.df
        cleandf = df_utils.remove_rolling_outliers(df,window = '1min',std_thresh=4)
        resampled_df = cleandf.resample('1min').mean()
        resampled_df = resampled_df.rename(columns=ggg_column_map)

        resampled_df['UTCDate'] = resampled_df.index.strftime('%y/%m/%d')
        resampled_df['UTCTime'] = resampled_df.index.strftime('%H:%M:%S')

        for col in ggg_column_order[2:]:
            if col not in resampled_df.columns:
                resampled_df[col] = -99.99
            resampled_df[col] = resampled_df[col].round(2)

        resampled_df = resampled_df[ggg_column_order]
        return resampled_df

class VaisalaTPH():
    raw_file_pattern = re.compile(r'\d{4}\d{2}\d{2}_tph\.txt')
    tz = pytz.timezone('UTC')

    def __init__(self,data_path):
        self.data_path = data_path

    def load_df_in_range(self,dtr):
        in_tz = dtr.tz
        if in_tz != self.tz:
            new_dtr = dtr.new_tz(self.tz)
        else:
            new_dtr = dtr

        dates = new_dtr.get_dates_in_range()
        data = []
        for date in dates:
            fname = self.create_raw_fname(date)
            try:
                df = self.load_df_from_raw_file(fname)
            except FileNotFoundError:
                continue
            data.append(df)

        return pd.concat(data)

    def create_raw_fname(self,date):
        return f'{date.strftime("%Y%m%d")}_tph.txt'

    def load_df_from_raw_file(self,fname,alternate_path=None):
        if alternate_path:
            full_filepath = os.path.join(alternate_path,fname)
        else:
            full_filepath = os.path.join(self.data_path,fname)
        if not self.raw_file_pattern.match(os.path.basename(full_filepath)):
            raise ValueError(f'Invalid file name: {full_filepath}')
        with open(full_filepath,'r') as f:
            lines = f.readlines()

        data = []
        for line in lines:
            parsed_data = self.parse_line(line)
            if parsed_data:
                data.append(parsed_data)
        df = pd.DataFrame(data)
        return df
        
    def parse_line(self,line):
        line = line.strip()
        if len(line)==0:
            return None
        try:
            et = float(line.split()[1])
            dt = datetime.datetime.utcfromtimestamp(et)
            dt = pytz.utc.localize(dt).astimezone(self.tz)
            p = float(line.split()[6])
            t = float(line.split()[9])
            rh = float(line.split()[12])
            return {
            'et': et,
            'dt': dt,
            'pres': p,
            'temp': t,
            'rh': rh
            }
        except (IndexError, ValueError):
            return None
