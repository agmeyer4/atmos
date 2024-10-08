"""Utilities for handling meteorological data

Classes:
    MetHandler: Class for handling meteorological data.
    GGGMetHandler: Class for handling meteorological data for GGG.
    VaisalaTPH: Class for handling Vaisala TPH data.
    LANLZeno: Class for handling LANL Zeno data.

"""

#Import outside dependencies
import pandas as pd
import datetime 
import numpy as np
import os 
import sys
import pytz
import re

#Import local dependencies
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
from utils import df_utils
from utils import datetime_utils

class MetConfig:
    """Configuration for met tools.

    Attributes:
        default_vars (dict): Dictionary containing the default variables for meteorological data.
    """

    def __init__(self,config_mode = 'default'):
        if config_mode == 'default':
            self.default_vars = {
                'et': {'description': 'Epoch time -- seconds since 1970-01-01 00:00:00 UTC', 'units': 's', 'dtype':'float'},
                'dt': {'description': 'Date time', 'units': 'na', 'dtype':'datetime.datetime'},
                'pres': {'description': 'Atmospheric Pressure','units': 'hPa', 'dtype':'float'},
                'temp': {'description': 'Temperature','units': 'C', 'dtype':'float'},
                'rh': {'description': 'Relative Humidity','units': '%', 'dtype':'float'},
                'ws': {'description': 'Wind Speed','units': 'm/s', 'dtype':'float'},
                'wd': {'description': 'Wind Direction (clockwise from N)','units': 'degrees', 'dtype':'float'},
                'u': {'description': 'u component of wind (>0 = from west)','units': 'm/s', 'dtype':'float'},
                'v': {'description': 'v component of wind (>0 = from south)','units': 'm/s', 'dtype':'float'},
                'w': {'description': 'w component of wind (>0 = upward)','units': 'm/s', 'dtype':'float'},
            }   
        else:
            raise ValueError("Invalid config_mode. Only have 'default' set up right now.")

class MetHandler(MetConfig):
    """Parent class for handling meteorological data.
    """

    def __init__(self,config_mode='default'):
        """Initialize the MetHandler object.
        
        Args:
            config_mode (str): Configuration mode to use. Default is 'default'.
        """

        super().__init__(config_mode)
    
    def load_stddata_in_range(self,met_type,data_path,dtr=None,start_dt=None,end_dt=None,tz='UTC'):
        """Load meteorological data in a specified datetime range.
        
        You can provide either a DateTimeRange object or both start_dt and end_dt.
                
        Args:
            met_type (str): Type of meteorological data. Options are 'vaisala_tph' and 'lanl_zeno'.
            data_path (str): Path to the meteorological data.
            dtr (DateTimeRange): DateTimeRange object specifying the datetime range.
            start_dt (datetime.datetime): Start of the datetime range.
            end_dt (datetime.datetime): End of the datetime range.
            tz (str): Timezone of the datetime range. Default is 'UTC'.
            
        Returns:
            pd.DataFrame: Dataframe containing the meteorological data.
        
        Raises:
            ValueError: If a DateTimeRange object or both start_dt and end_dt are not provided.
        """

        if dtr is None: #If a DateTimeRange object is not provided
            if start_dt is None or end_dt is None: #If both start_dt and end_dt are not provided
                raise ValueError("Either a DateTimeRange object or both start_dt and end_dt must be provided.")
            dtr = datetime_utils.DateTimeRange(start_dt, end_dt, tz) #Create a DateTimeRange object
        if not os.path.isdir(data_path): #If the data path isn't a dir
            raise ValueError(f"Invalid data path: {data_path}") #Raise an error

        if met_type == 'vaisala_tph': #If the meteorological data is Vaisala TPH
            mettypehandler = VaisalaTPH(data_path) #Create a VaisalaTPH object
        elif met_type == 'lanl_zeno': #If the meteorological data is LANL Zeno
            mettypehandler = LANLZeno(data_path) #Create a LANLZeno object
        elif met_type == 'ggg': #If the meteorological data is GGG
            mettypehandler = GGGMetHandler('loader',data_path) #Create a GGGMetHandler object
        else:
            raise ValueError("Invalid met_type. Only have 'vaisala_tph', 'lanl_zeno', and 'ggg' set up right now.")
        
        df = mettypehandler.load_df_in_range(dtr) #Load the data in the specified datetime range
        if len(df) == 0: #If there is no data
            print('Warning: No data found in specified datetime range.') #Print a warning
            return df
        df = self.standardize(df) #Standardize the dataframe
        if df.index.tz != dtr.tz:  #If the timezone of the dataframe does not match the timezone of the DateTimeRange object
            df.index = df.index.tz_convert(dtr.tz) #Convert the timezone of the dataframe
        df = df.loc[dtr.start_dt:dtr.end_dt] #Filter the dataframe to the specified datetime range
        return df
    

    def standardize(self,df):
        """Standardize the meteorological data.
        
        Args:
            df (pd.DataFrame): Dataframe containing the meteorological data.
            
        Returns:
            pd.DataFrame: Standardized dataframe ensuring that the column name and index are consistent with the default variables from metconfig
            
        Raises:
            ValueError: If the dataframe does not have a 'dt' column or a datetime index named 'dt'.
        """

        if 'dt' in df.columns: #If the dataframe has a 'dt' column
            df.set_index('dt', inplace=True) #Set the 'dt' column as the index
        elif df.index.name != 'dt': #If the index is not named 'dt'
            raise ValueError("DataFrame must have a 'dt' column or a datetime index named 'dt'.") #Raise an error
        extra_columns = [col for col in df.columns if col not in self.default_vars.keys()] #Find the columns that are not in the default variables
        if extra_columns: #If there are extra columns
            print(f"Warning: Extra columns in DataFrame that are not in default_vars: {extra_columns}") #Print a warning
        return df
    
class GGGMetHandler():
    """Class for handling meteorological data for GGG. It is intended to be used either to transform data standardized by MetHandler and
    write daily GGG meteorological files, or to read in GGG style met files and transform them into a standardized dataframe.

    Attributes:
        ggg_column_map (dict): Dictionary mapping the default column names to the GGG column names.
        ggg_column_order (list): Order of the columns in the GGG file.
    """

    #Map the default column names (in MetHandler) to the GGG column names
    ggg_column_map = {
        'pres': 'Pout',
        'temp': 'Tout',
        'rh': 'RH',
        'ws': 'WSPD',
        'wd': 'WDIR',
    }
    ggg_column_order = ['UTCDate','UTCTime','Pout','Tout','RH','WSPD','WDIR'] #Order of the columns in the GGG file
    tz = pytz.timezone('UTC') #Timezone of GGG data
    raw_file_pattern = re.compile(r'\d{8}[_\.]\w+\.txt') # Regular expression pattern for the raw file name -- e.g. 20210101_vtph.txt or 20210101.WBB.txt


    def __init__(self,mode,data_path = None):
        if mode == 'loader':
            if data_path is None:
                raise ValueError("Must provide a data path if using loader") 
            self.data_path = data_path
        elif mode == 'converter':
            self.data_path = data_path
        else:
            raise ValueError("Invalid mode. Must be 'loader' or 'converter'")
        
    def load_df_in_range(self,dtr=None,start_dt=None,end_dt=None,tz='UTC'):
        in_tz = dtr.tz #Timezone of the input DateTimeRange object
        if in_tz != self.tz : #If the timezone of the DateTimeRange object is not UTC
            new_dtr = dtr.new_tz(self.tz) #Create a new DateTimeRange object with the timezone set to UTC
        else:
            new_dtr = dtr #Otherwise, use the input DateTimeRange object

        dates = new_dtr.get_dates_in_range() #Get the dates in the specified datetime range
        data = [] #List to store the dataframes
        for date in dates: #Iterate over the dates
            fname = self.create_raw_fname(date) #Create the raw file name
            if fname is None: #If the file name is None
                continue #Skip to the next date
            try:
                df = self.load_df_from_raw_file(fname) #Load the dataframe from the raw file
            except FileNotFoundError: #If the file is not found
                continue #Skip to the next date
            data.append(df) #Append the dataframe to the list

        if len(data) == 0: #If there is no data 
            return pd.DataFrame() #Return an empty dataframe
        else:
            return pd.concat(data) #Concatenate the dataframes
    
    def create_raw_fname(self,date):
        """Create the raw file name for a given date, using the files in the data path provided.

        Args:
            date (datetime.datetime): Date for which to create the raw file name.

        Returns:
            str: Raw file name.
        """
        matching_dates = [] #List to store the matching dates
        for fname in os.listdir(self.data_path):
            if fname.startswith(date.strftime('%Y%m%d')) and self.raw_file_pattern.match(fname): #If the file name matches the date and the pattern
                matching_dates.append(fname) #Append the file name to the list
        if len(matching_dates)>1:
            raise ValueError(f"Multiple files found for date {date.strftime('%Y%m%d')}: {matching_dates}")
        elif len(matching_dates)==0:
            print(f'Warning: no file found for date {date.strftime("%Y%m%d")}')
            return None
        else:
            return matching_dates[0]    
    
    def load_df_from_raw_file(self,fname):
        """Load the GGG meteorological data from a raw file.

        Args:
            fname (str): Raw file name. Just the name.

        Returns:
            pd.DataFrame: Dataframe containing the GGG meteorological data.

        Raises:
            ValueError: If the file name is invalid.
        """

        full_filepath = os.path.join(self.data_path,fname) #Create the full file path using the self.data_path
        if not self.raw_file_pattern.match(os.path.basename(full_filepath)): #If t he file name is invalid
            raise ValueError(f'Invalid file name: {full_filepath}') #Raise an error

        df = pd.read_csv(full_filepath) #Read the CSV file
        df['dt'] = pd.to_datetime(df['UTCDate']+df['UTCTime'],format='%y/%m/%d%H:%M:%S').dt.tz_localize('UTC')
        df = df.drop(columns = ['UTCDate','UTCTime'])
        df = df.rename(columns={v: k for k, v in self.ggg_column_map.items()})
        df = df.replace(-99.99,np.nan)
        df = df.replace(-99.0,np.nan)
        return df

    def write_daily_ggg_met_files(self,df,met_type,write_path,overwrite=False):
        """Write daily GGG meteorological files.
        
        Args:
            df (pd.DataFrame): Dataframe containing the meteorological data.
            met_type (str): Type of meteorological data. Options are 'vaisala_tph' and 'lanl_zeno'.
            write_path (str): Path to write the GGG meteorological files.
            overwrite (bool): Whether to overwrite existing files. Default is False.
            
        """

        if df.index.tz != pytz.UTC: #If the timezone of the dataframe is not UTC
            df.index = df.index.tz_convert(pytz.UTC) #Convert the timezone to UTC
        ggg_df = self.prep_df_for_ggg(df) #Prepare the dataframe for GGG
        daily_dfs = [part for _, part in ggg_df.groupby(pd.Grouper(freq='1D')) if not part.empty] #parse into a list of daily dataframes
        for day_df in daily_dfs: #Iterate over the daily dataframes
            self.write_ggg_met_file(day_df,met_type,write_path,overwrite) #Write the GGG meteorological file

    def write_ggg_met_file(self,day_df,met_type,write_path,overwrite=False):
        """Write a GGG meteorological file.

        Args:
            day_df (pd.DataFrame): Dataframe containing the meteorological data for one day.
            write_path (str): Path to write the GGG meteorological file.
            overwrite (bool): Whether to overwrite existing files. Default is False.

        Raises:
            ValueError: If the dataframe index is not in UTC or contains data from more than one day.
            FileExistsError: If the file already exists.
        """

        if day_df.index.tz != pytz.UTC:
            raise ValueError("DataFrame index must be in UTC.")
        
        unique_dates = pd.Index(day_df.index.date).unique() #Get the unique dates in the dataframe index
        if len(unique_dates) != 1: #If there is more than one unique date
            raise ValueError("DataFrame index must contain data from only one day.") #Raise an error

        #Determine the file extension based on the meteorological data type
        if met_type == 'vaisala_tph': 
            file_ext = '_vtph.txt'
        elif met_type == 'lanl_zeno':
            file_ext = '_zeno.txt'
        else:
            file_ext = '.txt'
        
        full_fname = os.path.join(write_path,unique_dates[0].strftime('%Y%m%d')+file_ext) #Create the full file path     
        if os.path.exists(full_fname) and not overwrite:  #If the file already exists and overwrite is False
            raise FileExistsError(f"File already exists: {full_fname}") #Raise an error 

        day_df = day_df.dropna() #Drop any NaN values
        if len(day_df) == 0: #If the dataframe is empty, we don't want to write anything
            return
        
        #Otherwise, write the dataframe to a CSV file
        with open(full_fname,'w') as f: 
            day_df.to_csv(f,sep=',',index = False)

    def prep_df_for_ggg(self,df):
        """Prepare the dataframe  for GGG.
        
        Args:
            df (pd.DataFrame): Dataframe containing the meteorological data.

        Returns:
            pd.DataFrame: Dataframe prepared for GGG.
        """
 
        cleandf = df_utils.remove_rolling_outliers(df,window = '1min',std_thresh=4) #Remove rolling outliers
        resampled_df = cleandf.resample('1min').mean() #Resample the dataframe to 1 minute intervals 
        resampled_df = resampled_df.rename(columns=self.ggg_column_map) #Rename the columns

        resampled_df['UTCDate'] = resampled_df.index.strftime('%y/%m/%d') #Add the UTCDate column
        resampled_df['UTCTime'] = resampled_df.index.strftime('%H:%M:%S') #Add the UTCTime column

        #Fill in missing columns with -99.99
        for col in self.ggg_column_order[2:]:
            if col not in resampled_df.columns:
                resampled_df[col] = -99.99
            resampled_df[col] = resampled_df[col].round(2)

        resampled_df = resampled_df[self.ggg_column_order] #Reorder the columns and drop any extra columns
        return resampled_df

class VaisalaTPH():
    """Class for handling Vaisala TPH data.
    
    Attributes:
        raw_file_pattern (re.Pattern): Regular expression pattern for the raw file name.
        tz (pytz.tzinfo.BaseTzInfo): Timezone of the datetime range.
        data_path (str): Path to the meteorological data.
    """

    raw_file_pattern = re.compile(r'\d{4}\d{2}\d{2}_tph\.txt') #Regular expression pattern for the raw file name -- e.g. 20210101_tph.txt
    tz = pytz.timezone('UTC') #Timezone of Vaisala data

    def __init__(self,data_path): 
        self.data_path = data_path #Path to the meteorological data

    def load_df_in_range(self,dtr):
        """Load the Vaisala TPH data in a specified datetime range.

        Args:
            dtr (DateTimeRange): DateTimeRange object specifying the datetime range.

        Returns:
            pd.DataFrame: Dataframe containing the Vaisala TPH data.
        """

        in_tz = dtr.tz #Timezone of the input DateTimeRange object
        if in_tz != self.tz: #If the timezone of the DateTimeRange object is not UTC
            new_dtr = dtr.new_tz(self.tz) #Create a new DateTimeRange object with the timezone set to UTC
        else:
            new_dtr = dtr #Otherwise, use the input DateTimeRange object

        dates = new_dtr.get_dates_in_range() #Get the dates in the specified datetime range
        data = [] #List to store the dataframes
        for date in dates: #Iterate over the dates
            fname = self.create_raw_fname(date) #Create the raw file name
            try:
                df = self.load_df_from_raw_file(fname) #Load the dataframe from the raw file
            except FileNotFoundError: #If the file is not found
                continue #Skip to the next date
            data.append(df) #Append the dataframe to the list
        if len(data) == 0: #If there is no data 
            return pd.DataFrame() #Return an empty dataframe
        else:
            return pd.concat(data) #Concatenate the dataframes

    def create_raw_fname(self,date):
        """Create the raw file name for a given date.

        Args:
            date (datetime.datetime): Date for which to create the raw file name.

        Returns:
            str: Raw file name.
        """

        return f'{date.strftime("%Y%m%d")}_tph.txt'

    def load_df_from_raw_file(self,fname,alternate_path=None):
        """Load the Vaisala TPH data from a raw file.

        Args:
            fname (str): Raw file name. Just the name. 
            alternate_path (str): Alternate path to the raw file. Default is None, meaning it will use the self.data_path.

        Returns:
            pd.DataFrame: Dataframe containing the Vaisala TPH data.

        Raises:
            ValueError: If the file name is invalid.
        """

        if alternate_path: #If an alternate path is provided
            full_filepath = os.path.join(alternate_path,fname) #Create the full file path using the alternate path
        else: #Otherwise
            full_filepath = os.path.join(self.data_path,fname) #Create the full file path using the self.data_path
        if not self.raw_file_pattern.match(os.path.basename(full_filepath)): #If t he file name is invalid
            raise ValueError(f'Invalid file name: {full_filepath}') #Raise an error
        
        with open(full_filepath,'r') as f: #Open the file
            lines = f.readlines() #Read the lines

        data = [] #List to store the data
        for line in lines: #Iterate over the lines
            parsed_data = self.parse_line(line) #Parse the line
            if parsed_data: #If the parsed data is not None
                data.append(parsed_data) #Append the parsed data to the list
        df = pd.DataFrame(data) #Create a dataframe from the list
        return df
        
    def parse_line(self,line):
        """Parse a line from the raw file.

        Args:
            line (str): Line from the raw file.

        Returns:
            dict: Dictionary containing the parsed data that can be used to create a dataframe.
        """

        line = line.strip() #Strip the line of newlines 
        if len(line)==0:  #If the line is empty
            return None #Return None 
        try: #Try to parse the line
            et = float(line.split()[1]) #Extract the epoch time
            dt = datetime.datetime.utcfromtimestamp(et) #Convert the epoch time to a datetime object
            dt = pytz.utc.localize(dt).astimezone(self.tz) #Convert the datetime object to the UTC timezone
            p = float(line.split()[6]) #Extract the pressure 
            t = float(line.split()[9]) #Extract the temperature 
            rh = float(line.split()[12]) #Extract the relative humidity 
            return {
            'et': et,
            'dt': dt,
            'pres': p,
            'temp': t,
            'rh': rh
            } #Return a dictionary with the parsed data
        except (IndexError, ValueError): #If there is an error
            return None #Return None 


class LANLZeno():
    """Class for handling LANL Zeno data.

    Attributes:
        raw_file_pattern (re.Pattern): Regular expression pattern for the raw file name.
        tz (pytz.tzinfo.BaseTzInfo): Timezone of the datetime range.
        data_path (str): Path to the meteorological data.
    """

    raw_file_pattern = re.compile(r'weather-\d{4}-\d{2}-\d{2}\.txt') #Regular expression pattern for the raw file name -- e.g. weather-2021-01-01.txt
    tz = pytz.timezone('UTC') #Timezone of LANL Zeno data

    def __init__(self,data_path):
        self.data_path = data_path

    def load_df_in_range(self,dtr):
        """Load the LANL Zeno data in a specified datetime range.
        
        Args:
            dtr (DateTimeRange): DateTimeRange object specifying the datetime range.
        
        Returns:
            pd.DataFrame: Dataframe containing the LANL Zeno data.
        """

        in_tz = dtr.tz #Timezone of the input DateTimeRange object
        if in_tz != self.tz: #If the timezone of the DateTimeRange object is not UTC 
            new_dtr = dtr.new_tz(self.tz) #Create a new DateTimeRange object with the timezone set to UTC 
        else: #Otherwise      
            new_dtr = dtr #Use the input DateTimeRange object 

        dates = new_dtr.get_dates_in_range() #Get the dates in the specified datetime range 
        data = [] #List to store the dataframes
        for date in dates: #Iterate over the dates
            fname = self.create_raw_fname(date) #Create the raw file name 
            try:
                df = self.load_df_from_raw_file(fname) #Load the dataframe from the raw file
            except FileNotFoundError: #If the file is not found
                continue #Skip to the next date
            data.append(df) #Append the dataframe to the list

        if len(data) == 0: #If there is no data 
            return pd.DataFrame() #Return an empty dataframe
        else:
            return pd.concat(data) #Concatenate the dataframes

    def create_raw_fname(self,date):
        """Create the raw file name for a given date.

        Args:
            date (datetime.datetime): Date for which to create the raw file name.

        Returns:
            str: Raw file name.
        """

        return f'weather-{date.strftime("%Y-%m-%d")}.txt'

    def load_df_from_raw_file(self,fname,alternate_path=None,offsets = {'pres': -0.2}):
        """Load the LANL Zeno data from a raw file.
        
        Args:
            fname (str): Raw file name. Just the name.
            alternate_path (str): Alternate path to the raw file. Default is None, meaning it will use the self.data_path.
            offsets (dict): Dictionary of offsets to apply to the data. Default is {'pres': -0.2}.
            
        Returns:
            pd.DataFrame: Dataframe containing the LANL Zeno data.
            
        Raises:
            ValueError: If the file name is invalid.
        """
          
        if alternate_path: #If an alternate path is provided
            full_filepath = os.path.join(alternate_path,fname) #Create the full file path using the alternate path
        else: #Otherwise
            full_filepath = os.path.join(self.data_path,fname) #Create the full file path using the self.data_path
        if not self.raw_file_pattern.match(os.path.basename(full_filepath)): #If the file name is invalid
            raise ValueError(f'Invalid file name: {full_filepath}') #Raise an error 
        with open(full_filepath,'r') as f: #Open the file
            lines = f.readlines() #Read the lines

        data = [] #List to store the data
        for line in lines:  #Iterate over the lines
            parsed_data = self.parse_line(line) #Parse the line
            if parsed_data: #If the parsed data is not None
                data.append(parsed_data) #Append the parsed data to the list
        df = pd.DataFrame(data) #Create a dataframe from the list
        for key in offsets.keys(): #Iterate over the keys in the offsets dictionary
            df[key] = df[key] + offsets[key] 
        return df 
        
    def parse_line(self,line): 
        """Parse a line from the raw file.

        Args:
            line (str): Line from the raw file.

        Returns:
            dict: Dictionary containing the parsed data that can be used to create a dataframe.
        """

        line = line.strip() #Strip the line of newlines
        if len(line)==0: #If the line is empty
            return None #Return None
        splitline = line.split(',') #Split the line by commas
        try: #Try to parse the line
            datestr = splitline[1] #Extract the date string
            timestr = splitline[2] #Extract the time string
            dt = datetime.datetime.strptime(f'{datestr} {timestr}','%y/%m/%d %H:%M:%S') #Convert the date and time strings to a datetime object
            dt = pytz.utc.localize(dt).astimezone(self.tz) #Convert the datetime object to the UTC timezone
            p = float(splitline[12]) #Extract the pressure
            t = float(splitline[10]) #Extract the temperature
            rh = float(splitline[11]) #Extract the relative humidity
            return {
            'dt': dt,
            'pres': p,
            'temp': t,
            'rh': rh
            } #Return a dictionary with the parsed data
        except (IndexError, ValueError): #If there is an error
            return None #Return None
        
