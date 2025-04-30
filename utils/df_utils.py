""" This module contains utility functions for working with pandas dataframes. 

Functions:
    remove_rolling_outliers: Remove outliers from a dataframe using a rolling median and standard deviation.

"""

import pandas as pd
import numpy as np

def remove_rolling_outliers(df, window = '1h', columns='all', std_thresh=3):
    """ Remove outliers from a dataframe using a rolling median and standard deviation.
    
    Args:
        df (pd.DataFrame): Dataframe to remove outliers from.
        window (str or int): Size of the rolling window. If a string, it should be a pandas offset alias (e.g. '1h', '1D'). If an integer, it should be the number of rows in the window.
        columns (list or str): Columns to remove outliers from. If 'all', all columns will be used.
        std_thresh (int): Number of standard deviations from the rolling median to consider as an outlier.
    
    Returns:
        pd.DataFrame: Dataframe with outliers replaced with np.nan.
    """

    out_df = df.copy(deep = True) #Make a copy of the dataframe to avoid modifying the original dataframe
    if columns == 'all': #If all columns should be used
        columns = df.columns    

    for col in columns: #Iterate over the columns
        if not pd.api.types.is_numeric_dtype(out_df[col]):
            continue
        rolling_median = out_df[col].rolling(window=window, center=True,min_periods = 1).median() #Calculate the rolling median
        rolling_std = out_df[col].rolling(window=window, center=True,min_periods=1).std() #Calculate the rolling standard deviation
        outliers = (out_df[col] - rolling_median).abs() > std_thresh * rolling_std #Find the outliers
        out_df.loc[outliers, col] = np.nan #Replace the outliers with np.nan
    return out_df

def separate_daily_dfs(df,dt_col=None):
    """
    Separate the dataframe into daily dataframes.

    Args:
        df (pd.DataFrame): The dataframe to be separated.
        dt_col (str): The name of the datetime column. If None, the index is used.

    Returns:
        dict: A dictionary where the keys are dates and the values are dataframes for that date.
    """
    daily_dfs = {}
    if dt_col is not None:
        df[dt_col] = pd.to_datetime(df[dt_col])
        df.set_index(dt_col, inplace=True)
    else:
        df.index = pd.to_datetime(df.index)

    for date, group in df.groupby(df.index.date):
        datestr = date.strftime('%Y-%m-%d')
        daily_dfs[datestr] = group
    return daily_dfs