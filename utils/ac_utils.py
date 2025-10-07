'''
Module: ac_utils.py
Author: Aaron G. Meyer (agmeyer4@gmail.com)
Description: This module contains functions and classes for doing atmospheric column analysis as part of the 
atmos package.
'''

###################################################################################################################################
# Import Necessary Packages
###################################################################################################################################
import os
import shutil
import datetime
import pytz
import pandas as pd
import numpy as np
import xarray as xr
from . import df_utils

###################################################################################################################################
# Define Functions     
###################################################################################################################################
def copy_em27_oofs_to_singlefolder(existing_results_path,oof_destination_path):
    '''Copies oof files from the EM27 results path to a single directory
    
    Args:
    existing_results_path (str) : path to the results folder. Within this folder should be a 'daily' and further date subfolders.
    oof_destination_path (str) : path to the folder where the oof files will be copied
    '''

    files_to_transfer = []
    daily_results_folder = os.path.join(existing_results_path,'daily') 
    for datefolder in os.listdir(daily_results_folder):
        for file in os.listdir(os.path.join(daily_results_folder,datefolder)):
            if file.endswith('.vav.ada.aia.oof'):
                full_filepath = os.path.join(daily_results_folder,datefolder,file)
                files_to_transfer.append(full_filepath)
    for full_filepath in files_to_transfer:
        shutil.copy(full_filepath,oof_destination_path)

def find_sbs_ranges(inst_details,inst_id1,inst_id2):
    '''Finds the date ranges where two instruments have overlapping data collection periods

    Args:
    inst_details (dict) : dictionary of instrument details like {'inst_id':[{'site':site,'dtr':dtr},...],...} where dtr is a datetime_utils.DateTimeRange object
    inst_id1 (str) : instrument id of the first instrument
    inst_id2 (str) : instrument id of the second instrument

    Returns:
    sbs_details (dict) : dictionary of the overlapping date ranges by site like {'site':[dtr1,dtr2,...],...} where dtr is a datetime_utils.DateTimeRange object
    '''

    inst_det_list1 = inst_details[inst_id1] #list of dicts with site and dtr for inst_id1
    inst_det_list2 = inst_details[inst_id2] #list of dicts with site and dtr for inst_id2
    sbs_details = {} #initialize the output dict
    for inst_det1 in inst_det_list1:
        for inst_det2 in inst_det_list2:
            if inst_det1['site'] == inst_det2['site']: #only look for overlaps if the sites are the same
                site = inst_det1['site']
            else:
                continue
            dtr1 = inst_det1['dtr']
            dtr2 = inst_det2['dtr']
            sbs_dtr = dtr1.intersection(dtr2) #find the intersection of the two date ranges, will be None if there is no intersection
            if sbs_dtr is not None: 
                try:
                    sbs_details[site].append(sbs_dtr) #append the intersection to the list for that site
                except KeyError:  #if the site is not already in the dict, add it
                    sbs_details[site] = [sbs_dtr]
            
    return sbs_details

def find_sbs_ranges_inrange(inst_details,inst_id1,inst_id2,dtrange):
    '''Finds the date ranges where two instruments have overlapping data collection periods within a given date range
    Args:
    inst_details (dict) : dictionary of instrument details like {'inst_id':[{'site':site,'dtr':dtr},...],...} where dtr is a datetime_utils.DateTimeRange object
    inst_id1 (str) : instrument id of the first instrument
    inst_id2 (str) : instrument id of the second instrument
    dtrange (datetime_utils.DateTimeRange) : date range to limit the search to

    Returns:
    sub_site_details (dict) : dictionary of the overlapping date ranges by site like {'site':[dtr1,dtr2,...],...} where dtr is a datetime_utils.DateTimeRange object
    '''

    sbs_details = find_sbs_ranges(inst_details,inst_id1,inst_id2)
    sub_site_details = {}
    for site in sbs_details.keys():
        for sbs_dtr in sbs_details[site]:
            intersection = sbs_dtr.intersection(dtrange)
            if intersection is not None:
                try:
                    sub_site_details[site].append(intersection)
                except KeyError:
                    sub_site_details[site] = [intersection]

    return sub_site_details

def merge_oofdfs(oof_dfs,dropna=False):
    '''Function to merge oof dfs and add the instrument id as a suffix to each column name. Generally we use
    this when comparing two oof dfs, such as correcting one to another via an offset. 
    
    Args: 
    oof_dfs (dict): dictionary of oof dataframes, where the key is the instrument id (like 'ha') and the value is the dataframe
    dropna (bool): True if we want to drop na values (when joining, often there are many), False if not. Default False
    
    Returns:
    merged_df (pd.DataFrame): pandas dataframe of the merged oof dfs, with suffixes for instrument id added to each column
    '''

    inst_ids = oof_dfs.keys() #get the instrument ids
    dfs_for_merge = {} #initialize the dataframes for merging
    for inst_id in inst_ids: #loop through the instrument ids
        df = oof_dfs[inst_id].copy() #copy the dataframe for that instrument id
        for col in df.columns: #for each of the columns in the dataframe
            df = df.rename(columns={col:f'{col}_{inst_id}'}) #add the instrument id as a suffix to the original column name after a _
        dfs_for_merge[inst_id] = df #add this dataframe to the merge dict
    merged_df = pd.DataFrame() #initialize the merged dataframe
    for inst_id in inst_ids: #for all of the dataframes
        merged_df = pd.concat([merged_df,dfs_for_merge[inst_id]],axis = 1) #merge them into one by concatenating
    if dropna: #if we want to drop the na values,
        merged_df = merged_df.dropna() #do it
    return merged_df

def get_surface_ak(ds,xgas_name,airmass_name = 'o2_7885_am_o2'):
    """ Gets the surface averaging kernel for a given xgas from the xarray dataset (loaded from the ggg .nc file)
    
    Args:
        ds (xarray.Dataset) : xarray dataset loaded from the ggg .nc file
        xgas_name (str) : name of the gas to get the surface ak for, like 'co2(ppm)' or 'ch4(ppm)'
        airmass_name (str) : name of the airmass variable in the dataset, default is 'o2_7885_am_o2'

    Returns:
        surface_ak_interp (xarray.DataArray) : xarray DataArray of the surface ak for the given gas, interpolated to the slant column values
    """
    airmass = ds[airmass_name]
    xgas = ds[xgas_name]
    slant_xgas = xgas * airmass

    ak_surface = ds[f'ak_{xgas_name}'].isel(ak_altitude=0).values
    surface_interpolated_values = []
    slant_xgas_values = slant_xgas.values
    ak_slant_xgas_bin = ds[f'ak_slant_{xgas_name}_bin'].values

    for slant_value in slant_xgas_values:
        interpolated_surface_ak = np.interp(
            slant_value, #current slant value
            ak_slant_xgas_bin, #bin centers
            ak_surface, #ak values
        )
        surface_interpolated_values.append(interpolated_surface_ak)

    surface_ak_interp = xr.DataArray(
        surface_interpolated_values,
        dims='time',
        coords={'time':ds.time}
    )

    return surface_ak_interp

def div_by_ak(df,col_name,ak_col_name):
    """
    Divide a column in the DataFrame by the corresponding averaging kernel column.

    Args:
        df: DataFrame containing the data.
        col_name: Str, name of the column to be divided.
        ak_col_name: Str, name of the averaging kernel column.

    Returns:
        DataFrame with the new column containing the divided values.
    """
    
    out_df = df.copy()
    out_df[f'{col_name}_divak'] = out_df[col_name] / out_df[ak_col_name]
    return out_df

def add_rise_set(oof_df):
    '''Adds a column to the dataframe indicating if the sun is rising or setting at that time
    
    Args:
    oof_df (pd.DataFrame) : pandas dataframe of oof data, must have a column 'solzen(deg)' for the solar zenith angle
    
    Returns:
    out_oof_df (pd.DataFrame) : the same input dataframe with a new column 'rise_set' added, indicating if the sun is rising or setting
    '''
    
    out_oof_df = pd.DataFrame() #initialize the output dataframe
    oof_day_dfs = [part for _, part in oof_df.groupby(pd.Grouper(freq='1D')) if not part.empty] #parse into a list of daily dataframes
    for df in oof_day_dfs: #for each daily dataframe
        min_sza_idx = df['solzen(deg)'].idxmin() #find the sun's peak (min sza)
        df['rise_set'] = ['rise' if idx <= min_sza_idx else 'set' for idx in df.index] #add a column indicating if the sun is rising or setting (before or after peak)
        out_oof_df = pd.concat([out_oof_df,df]) #concatenate the daily dataframes into one

    return out_oof_df #return the concatenated dataframe

# Anomaly functions
def create_binned_summary(df,ak_df,sza_bin_size,gases):
    '''Creates a summary dataframe for binning values on sza for rising and setting sun. Used for getting "daily anomolies" per Wunch 2009
    
    Args:
    df (pd.DataFrame): pandas dataframe containing EM27 data
    sza_bin_size (int, float): size (in degrees) to make the solar zenith angle bins
    gases (list): list of strings corresponding to the species wanted. takes the mean of each species for the corresponding "rise" or "set for the sza bin
    
    Returns:
    binned_summary_df (pd.DataFrame): dataframe containing information about the sza binned data'''
    bins = np.arange(0,90,sza_bin_size) #create the bins
    df['sza_bin'] = pd.cut(df['solzen(deg)'],bins) #create a column in the dataframe indicating the bin that row belongs to
    grouped = df.groupby(['sza_bin','rise_set']) #group by the bin as well as rise_set so that they are two separate entities
    binned_summary = [] #initialize a list to store summary data
    for name, group in grouped:
        bin_dict = {} #initialize the binned group's data dict
        bin_dict['tstart'] = group.index[0] #start time of the bin
        bin_dict['tend'] = group.index[-1] #end time of the bin
        bin_dict['tmid'] = bin_dict['tstart']+(bin_dict['tend']-bin_dict['tstart'])/2 #middle time of the bin (can be used for plotting)
        bin_dict['sza_bin'] = name[0] #the sza bin itself
        bin_dict['nobs'] = len(group) #how many observations in that bin
        spec_bin_means = group.mean(numeric_only=True)[gases] #get the means of the species we want
        bin_dict.update({gas:spec_bin_means[gas] for gas in gases}) #add the means to the bin_dict
        bin_dict.update({f'{gas}_error':group.mean(numeric_only=True)[f'{gas}_error'] for gas in gases}) #add the std dev of the species to the bin_dict
        bin_dict.update({f'{gas}_std':group.std(numeric_only=True)[f'{gas}'] for gas in gases}) #add the std dev of the species to the bin_dict
        bin_dict['rise_set'] = name[1] #whether it is 'rise' or 'set'

        # Filter the averaging kernel DataFrame for the time range of the bin
        ak_filtered = ak_df[(ak_df.index >= bin_dict['tstart']) & (ak_df.index <= bin_dict['tend'])]
        if not ak_filtered.empty:
            for gas in gases:
                ak_col = f'{gas}_surf_ak'
                bin_dict[f'{gas}_surf_ak'] = ak_filtered[ak_col].mean()

        binned_summary.append(bin_dict) #append that group's info to the summary list
    binned_summary_df = pd.DataFrame(binned_summary)  #make the dataframe from the list of dicts
    binned_summary_df['sza_mid'] = binned_summary_df.apply(lambda row:row['sza_bin'].mid,axis = 1)
    return binned_summary_df

def daily_anomaly_creator(binned_summary_df,gases,co2_thresh):
    '''Create the daily anomoly
    
    Args:
    binned_summary_df (pd.DataFrame): created using create_binned_summary, contains daily summary information
    gases (list): list of the species names to use
    
    Returns:
    anom_df (pd.DataFrame): dataframe of the daily anomolies
    skipped_df (pd.DataFrame): dataframe containing information about bins that were skipped and why
    '''

    bin_grouped = binned_summary_df.groupby('sza_bin') #group by sza bin
    #initialize the data lists
    anom_list = []
    skipped_list = []
    for name,group in bin_grouped:
        if len(group)>2: #make sure there's not more than two rows -- should just be one rise and one set for that sza bin
            raise Exception('grouped df greater than 2 -- investigate') 
        if not all(item in group['rise_set'].values for item in ['rise', 'set']): #check that exactly one rise and one set rows exist in the df
            skipped_list.append(dict(skipmode = 'no_match_sza', #document if so
                                        sza_bin = name,
                                        rise_set = group['rise_set'].unique()))
            continue #if there aren't a rise and set, we can't do the anomoly, so just go to the next grouping
        if (group['nobs'].max()>(2*group['nobs'].min())): #check that there aren't more that 2x the number of observations for "rise" compared to set (or vice versa)
            skipped_list.append(dict(skipmode = 'nobs',
                                        sza_bin = name)) #ifso, document and continue
            continue
        rise_row = group.loc[group['rise_set']=='rise'].squeeze() #get the rise data (squeeze gets the value)
        set_row = group.loc[group['rise_set']=='set'].squeeze() #same with set
        anom_dict = {f'{gas}_anom':(set_row[gas] - rise_row[gas]) for gas in gases} #subtract rise from set for that sza, for each species in the list
        anom_dict.update({'sza_bin':name}) #update the dict with the sza bin
        anom_dict.update({f'{gas}_error' : np.mean(group[f'{gas}_error']) for gas in gases}) #add the error (mean of the two)
        anom_dict.update({f'{gas}_std' : np.mean(group[f'{gas}_std']) for gas in gases})
        anom_dict.update({f'{gas}_surf_ak' : np.mean(group[f'{gas}_surf_ak']) for gas in gases})
        anom_list.append(anom_dict) #append it to the list
        
    anom_df = pd.DataFrame(anom_list) #create the dataframe for the anomolie
    if len(anom_df) > 0: #if there are entries in the anom df, add a sza midpoint for plotting
        anom_df['sza_mid'] = anom_df.apply(lambda row: row['sza_bin'].mid,axis = 1)
        for gas in gases:
            anom_df = div_by_ak(anom_df,f'{gas}_anom',f'{gas}_surf_ak')
            anom_df = div_by_ak(anom_df,f'{gas}_error',f'{gas}_surf_ak')
            anom_df = div_by_ak(anom_df,f'{gas}_std',f'{gas}_surf_ak')
        anom_df['co2_above_thresh']  = (anom_df['xco2(ppm)_anom'].max()- anom_df['xco2(ppm)_anom'].min())>co2_thresh
    skipped_df = pd.DataFrame(skipped_list) #create the dataframe for why some sza's may have been skipped
    return anom_df, skipped_df

####################################################################################################################################
# Define Classes
####################################################################################################################################
class oof_manager:
    '''Class to manage getting data from oof files'''

    def __init__(self,oof_data_folder,timezone):
        '''
        Args: 
        oof_data_folder (str) : path to the folder where oof data is stored
        timezone (str) : timezone for the measurments
        '''
        self.oof_data_folder = oof_data_folder
        self.timezone = timezone

    def load_oof_df_inrange(self,dt1,dt2,filter_flag_0=False,print_out=False,cols_to_load=None):
        '''Loads a dataframe from an oof file for datetimes between the input values
        
        Args:
        dt1_str (str) : string for the start time of the desired range of form "YYYY-mm-dd HH:MM:SS" 
        dt2_str (str) : string for the end time of the desired range of form "YYYY-mm-dd HH:MM:SS" 
        oof_filename (str) : name of the oof file to load
        filter_flag_0 (bool) : True will filter the dataframe to rows where the flag column is 0 (good data), false returns all the data
        print_out (bool) : Will print a message telling the user that they are loading a certain oof file. Default False. 
        cols_to_load (list) : List of strings that are the names of the oof data columns to load. Default is None, which loads all of the columns. 

        Returns:
        df (pd.DataFrame) : pandas dataframe loaded from the oof files, formatted date, and column names       
        '''
        if type(dt1) == str:
            dt1 = self.tzdt_from_str(dt1)
            dt2 = self.tzdt_from_str(dt2)
        oof_files_inrange = self.get_oof_in_range(dt1,dt2)
        full_df = pd.DataFrame()
        for oof_filename in oof_files_inrange:
            if print_out:
                print(f'Loading {oof_filename}')
            df = self.df_from_oof(oof_filename,fullformat = True, filter_flag_0 = filter_flag_0, cols_to_load=cols_to_load) #load the oof file to a dataframe
            #df = self.df_dt_formatter(df) #format the dataframe to the correct datetime and column name formats
            df = df.loc[(df.index>=dt1)&(df.index<=dt2)] #filter the dataframe between the input datetimes
            #if filter_flag_0: #if we want to filter by flag
            #    df = df.loc[df['flag'] == 0] #then do it!
            full_df = pd.concat([full_df,df])
        return full_df

    def df_from_oof(self,filename,fullformat = False,filter_flag_0 = False,cols_to_load=None):
        '''Load a dataframe from an oof file
        
        Args:
        filename (str) : name of the oof file (not the full path)
        fullformat (bool) : if you want to do the full format
        filter_flag_0 (bool) : if you want to only get the 0 flags (good data), set to True
        cols_to_load (list) : list of strings of the oof columns you want to load. Default None which loads all of the columns
        
        Returns:
        df (pd.DataFrame) : a pandas dataframe loaded from the em27 oof file with applicable columns added/renamed
        '''

        oof_full_filepath = os.path.join(self.oof_data_folder,filename) #get the full filepath using the class' folder path
        header = self.read_oof_header_line(oof_full_filepath)
        if cols_to_load == None: #if use_cols is none, we load all of the columns into the dataframe
            df = pd.read_csv(oof_full_filepath,
                            header = header,
                            sep='\s+',
                            skip_blank_lines=False) #read it as a csv, parse the header
        else:
            must_have_cols = ['flag','year','day','hour','lat(deg)','long(deg)','zobs(km)'] #we basically always need these columns
            usecols = cols_to_load.copy() #copy the cols to load so it doesn't alter the input list (we often use "specs" or simlar)
            for mhc in must_have_cols: #loop through the must haves
                if mhc not in cols_to_load: #if they aren't in the columns to load
                    usecols.append(mhc) #add them 

            df = pd.read_csv(oof_full_filepath, #now load the dataframe with the specific columns defined
                header = header,
                sep='\s+',
                skip_blank_lines=False,
                usecols = usecols) #read it as a csv, parse the header
                
        df['inst_zasl'] = df['zobs(km)']*1000 #add the instrument z elevation in meters above sea level (instead of km)
        df['inst_lat'] = df['lat(deg)'] #rename the inst lat column
        df['inst_lon'] = df['long(deg)'] #rename the inst lon column 
        if fullformat:
            df = self.df_dt_formatter(df)
        if filter_flag_0:
            df = df.loc[df['flag']==0]
        return df

    def tzdt_from_str(self,dt_str):
        '''Apply the inherent timezone of the class to an input datetime string
        
        Args:
        dt_str (str) : datetime string of form "YYYY-mm-dd HH:MM:SS" 
        
        Returns:
        dt (datetime.datetime) : timezone aware datetime object, with timezone determined by the class
        '''

        dt = datetime.datetime.strptime(dt_str,'%Y-%m-%d %H:%M:%S') #create the datetime
        dt = pytz.timezone(self.timezone).localize(dt) #apply the timezone
        return dt

    def read_oof_header_line(self,full_file_path):
        '''Reads and parses the header line of an oof file
        
        Args: 
        full_file_path (str) : full path to an oof file we want to read
        
        Returns:
        header (list) : list of column names to use in the header 
        '''

        with open(full_file_path) as f: #open the file
            line1 = f.readline() #read the first line
        header = int(line1.split()[0])-1 #plit the file and get the header
        return header

    def parse_oof_dt(self,year,doy,hr_dec):
        '''Get a real datetime from an oof style datetime definition
        
        Args:
        year (int) : year
        doy (int) : day of the year 
        hr_dec (float) : decimal hour of the day
        
        Returns:
        dt (pandas.datetime) : pandas datetime object gleaned from the inputs
        '''

        dt = pd.to_datetime(f'{int(year)} {int(doy)}',format='%Y %j') + datetime.timedelta(seconds = hr_dec*3600)
        return dt

    def df_dt_formatter(self,df):
        '''Format a loaded oof dataframe to have the correct datetime as an index

        Assumes that the oof timestamps are in UTC
        
        Args: 
        df (pd.DataFrame) : dataframe loaded using df_from_oof() 

        Returns:
        df (pd.DataFrame) : reformatted dataframe with datetime as the index, and converted to a timezone aware object. 
        '''

        df['dt'] = np.vectorize(self.parse_oof_dt)(df['year'],df['day'],df['hour']) #set the datetime column by parsing the year, day and hour columns
        df = df.set_index('dt',drop=True).sort_index() #set dt as the index
        df.index = df.index.tz_localize('UTC').tz_convert(self.timezone) #localize and convert the timezone
        return df

    def get_sorted_oof(self):
        '''Get a list of oof files in the oof data folder, sorted so they are in chron order
        
        Returns:
        files (list) : list of files ending in oof in the data folder
        '''

        files = [] #initialize the list
        for file in sorted(os.listdir(self.oof_data_folder)): #loop through the sorted filenames in the oof data folder
            if file.endswith('oof'): #if the file ends in oof
                files.append(file) #add it to the list
        return files

    def get_oof_in_range(self,dt1,dt2):
        '''Finds the oof files in the data folder that fall between two input datetimes
        
        Args:
        dt1 (str or datetime.datetime) : start datetime of the interval we want to find files within
        dt2 (str or datetime.datetime) : end datetime of the interfal we want to find files within
        
        Returns:
        files in range (list) : list of oof filenames that fall within the datetime range input
        '''
        dt1 = dt1 - datetime.timedelta(days=1) #sometimes with UTC there are values in the previous day's oof file, so start one behind to check
        daystrings_in_range = [] #initialize the day strings in the range
        delta_days = dt2.date()-dt1.date() #get the number of days delta between the end and the start
        for i in range(delta_days.days +1): #loop through that number of days 
            day = dt1.date() + datetime.timedelta(days=i) #get the day by incrementing by i (how many days past the start)
            daystrings_in_range.append(day.strftime('%Y%m%d')) #append a string of the date (YYYYmmdd) to match with filenames

        files_in_range = [] #initilize the filenames that will be in the range
        for file in self.get_sorted_oof(): #loop through the sorted oof files in the data folder
            for daystring_in_range in daystrings_in_range: # loop through the daystrings that are in the range
                if daystring_in_range in file: #if the daystring is in the filename, 
                    files_in_range.append(file) #append it. Otherwise keep going
        
        return files_in_range

    def date_from_oof(self,oof_filename):
        '''Strips the date from an oof filename
        
        Args: 
        oof_filename (str)

        Returns:
        date (datetime.datetime.date) : date gained from the oof filename
        '''

        try:
            datestring = oof_filename.split('.')[0][2:] #split the oof_filename on . and remove the two letter identifier 
            date = datetime.datetime.strptime(datestring,"%Y%m%d").date() #convert to a date
            return date
        except:
            raise Exception(f'Error in getting datestring from {oof_filename}')

    def get_inrange_dates(self,dt1,dt2):
        '''Gets a range of dates between an input datetime range
        
        Args:
        dt1 (datetime.datetime) : start datetime
        dt2 (datetime.datetime) : end datetime
        
        Returns:
        dates_in_range (list) : list of dates within the datetime range
        '''

        files_in_range = self.get_oof_in_range(dt1,dt2) #find the files in the range
        dates_in_range = [] #initialize the dates list
        for oof_filename in files_in_range: #loop through the files in the range
            inrange_date = self.date_from_oof(oof_filename) #grab the date
            dates_in_range.append(inrange_date) #and append it
        return dates_in_range

    def check_get_loc(self,oof_df):
        '''Checks and gets the location of the instrument from the oof file
        TODO: This will break if the location moves during data collection or between days. This could become an issue if data was collected
        during one day and went past midnight UTC, then moved to a differnt location the next day. The oof_df in this case for the secnod day
        would include some data from the first data colleciton session in the early UTC hours, before moveing. 

        Args: 
        oof_df (pd.DataFrame) : dataframe of oof values
        
        Returns: 
        inst_lat (float) : instrument latitude
        inst_lon (float) : instrument longitude
        inst_zasl (float) : instrument elevation above sea level in meters        
        '''

        cols_to_check = ['inst_lat','inst_lon','inst_zasl']
        for col in cols_to_check:
            if not df_utils.pdcol_is_equal(oof_df[col]):
                raise Exception('{col} is not the same for the entire oof_df. This is an edge case.')
        #If we make it through the above, we can pull the values from the dataframe at the 0th index because they are all the same
        inst_lat = oof_df.iloc[0]['inst_lat']
        inst_lon = oof_df.iloc[0]['inst_lon']
        inst_zasl = oof_df.iloc[0]['inst_zasl']
        return inst_lat,inst_lon,inst_zasl   

def main():
    pass

if __name__ == "__main__":
   main()