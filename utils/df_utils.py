import numpy as np

def remove_rolling_outliers(df, window = '1h', columns='all', std_thresh=3):
    out_df = df.copy(deep = True)
    if columns == 'all':
        columns = df.columns

    for col in columns:
        rolling_median = out_df[col].rolling(window=window, center=True,min_periods = 1).median()
        rolling_std = out_df[col].rolling(window=window, center=True,min_periods=1).std()
        outliers = (out_df[col] - rolling_median).abs() > std_thresh * rolling_std
        out_df.loc[outliers, col] = np.nan
    return out_df
