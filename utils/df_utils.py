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

def resample_with_stats(df, col_name, err_col_name, freq, min_obs=3,resample_kwargs = {'label':'right','closed':'right'}):
    """
    Resample a given column and its associated error column, computing:
    - Mean
    - Standard Deviation
    - Weighted Mean (using 1/error^2 as weights)
    - Weighted Standard Deviation
    - Propagated Error: sqrt(sum(errors^2)) / count
    - Filters out bins with fewer than `min_obs` observations

    Parameters:
        df: DataFrame with a DateTime index
        col_name: str, column name to resample
        err_col_name: str, corresponding error column name
        freq: str, resampling frequency (default: '5T' for 5 minutes)
        min_obs: int, minimum number of observations required for a bin to be included
        resample_kwargs: dict, additional arguments for the resample method

    Returns:
        Resampled DataFrame with computed statistics, excluding bins with too few observations
    """
    def weighted_mean(x, errors):
        weights = 1 / (errors ** 2)
        return np.sum(weights * x) / np.sum(weights)

    def weighted_std(x, errors):
        weights = 1 / (errors ** 2)
        w_mean = weighted_mean(x, errors)
        return np.sqrt(np.sum(weights * (x - w_mean) ** 2) / np.sum(weights))

    def siqdivn(errors):
        return np.sqrt(np.sum(errors ** 2)) / len(errors)

    def sum_of_weights(errors):
        return 1 / np.sqrt((1 / errors ** 2).sum())


    if err_col_name is None:
        return df[col_name].resample(freq,**resample_kwargs).mean()

    resampled = df[[col_name, err_col_name]].resample(freq,**resample_kwargs).apply(
        lambda g: pd.Series({
            f'count': len(g),
            f'{col_name}_mean': g[col_name].mean(),
            f'{col_name}_std': g[col_name].std(),
            f'{col_name}_wmean': weighted_mean(g[col_name], g[err_col_name]),
            f'{col_name}_wstd': weighted_std(g[col_name], g[err_col_name]),
            f'{err_col_name}_siqdivn': siqdivn(g[err_col_name]),
            f'{err_col_name}_sumweights': sum_of_weights(g[err_col_name])
        })
    )

    # Apply filtering based on min_obs
    resampled = resampled[resampled['count'] >= min_obs]

    return resampled.drop(columns=['count'])

def subtract_quantile(df,col_names,quantile):
    """
    Subtract the quantile from the columns in the dataframe.

    Args:
        df (pd.DataFrame): The dataframe to be processed.
        col_names (list): List of column names to process.
        quantile (float): The quantile to subtract.

    Returns:
        pd.DataFrame: A new dataframe with the quantile subtracted from the specified columns.
    """

    out_df = df.copy()
    col_quantiles = {}
    for col in col_names:
        col_quantile = get_col_quantile(out_df,col,quantile)
        out_df[quant_col_label(col,quantile)] = out_df[col] - col_quantile
        col_quantiles[col] = col_quantile
    return out_df, col_quantiles

def quant_col_label(col,quantile):
    """
    Generate a label for the quantile column.

    Args:
        col (str): The original column name.
        quantile (float): The quantile value.

    Returns:
        str: The label for the quantile column.
    """

    return f'{col}_ex{int(quantile*100)}q'

def get_col_quantile(df,col,quantile):
    """
    Get the quantile value for a specific column in the dataframe.

    Args:
        df (pd.DataFrame): The dataframe to be processed.
        col (str): The column name.
        quantile (float): The quantile to calculate.

    Returns:
        float: The quantile value for the specified column.
    """
    if col not in df.columns:
        raise ValueError(f"Column {col} not found in the dataframe.")
    return df[col].quantile(quantile)

def rmv_prep(str):
    """
    Remove the prefix 'prep_' from a string.

    Args:
        str (str): The input string.

    Returns:
        str: The modified string with 'prep_' removed.
    """
    return '_'.join(str.split('_')[1:])

def get_season_df(df,season,date_col = None, seasons = {'DJF': [12, 1, 2],'MAM': [3, 4, 5],'JJA': [6, 7, 8],'SON': [9, 10, 11]}):
    season_months = seasons[season]
    if date_col is None:
        return df.loc[df.index.month.isin(season_months)]
    else:
        df['month'] = df.apply(lambda row: row[date_col].month,axis=1)
        return df.loc[df['month'].isin(season_months)]