"""Regression utilities for atmospheric data analysis."""

# Imports
import numpy as np
import scipy

# Functions
def linear_model(params, x):
    """Basic linear model.

    Args:
        params (list): List of parameters [slope, intercept].
        x (array-like): Independent variable.

    Returns:
        array: Dependent variable.
    """
    # Unpack parameters
    slope, yint = params
    return slope * x + yint

def get_R_squared(x,y,slope,intercept):
    """Calculate R-squared value.
    
    This function calculates the R-squared value for a linear regression model.
    The R-squared value indicates how well the independent variable explains the
    variability of the dependent variable. It is calculated as 1 minus the ratio
    of the residual sum of squares to the total sum of squares. 
    https://en.wikipedia.org/wiki/Coefficient_of_determination

    Args:
        x (array-like): Independent variable.
        y (array-like): Dependent variable.
        slope (float): Slope of the regression line.
        intercept (float): Intercept of the regression line.
        
    Returns:
        float: R-squared value.
    """
    y_pred = linear_model([slope,intercept],x)
    y_mean = np.mean(y)
    ss_res = np.sum((y - y_pred)**2)
    ss_tot = np.sum((y - y_mean)**2)
    r_squared = 1 - (ss_res/ss_tot)
    return r_squared

def get_r(x,y,slope,intercept):
    """Calculate Pearson correlation coefficient (r).

    This function calculates the Pearson correlation coefficient (r) for a linear
    regression model. The Pearson correlation coefficient measures the strength
    and direction of the linear relationship between two variables. It is calculated
    as the covariance of the two variables divided by the product of their standard
    deviations. The value of r ranges from -1 to 1, where -1 indicates a perfect
    negative linear relationship, 1 indicates a perfect positive linear relationship,
    and 0 indicates no linear relationship.
    #https://en.wikipedia.org/wiki/Pearson_correlation_coefficient

    Args:
        x (array-like): Independent variable.
        y (array-like): Dependent variable.
        slope (float): Slope of the regression line.
        intercept (float): Intercept of the regression line.

    Returns:
        float: Pearson correlation coefficient (r).
    """

    y_mean = np.mean(y)
    x_mean = np.mean(x)
    numerator = np.sum((x-x_mean)*(y-y_mean))
    denominator = np.sqrt(np.sum((x-x_mean)**2)*np.sum((y-y_mean)**2))
    return numerator/denominator

#Regressions
def ols_regression(df,x_name,y_name):
    """Perform Ordinary Least Squares (OLS) regression.

    This function performs Ordinary Least Squares (OLS) regression on the given
    DataFrame. It calculates the slope, intercept, R-squared value, Pearson correlation
    coefficient (r), p-value, and standard error of the regression line. 

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        x_name (str): Name of the independent variable column.
        y_name (str): Name of the dependent variable column.

    Returns:
        dict: Dictionary containing the regression results, including slope, intercept,
              R-squared value, Pearson correlation coefficient (r), p-value, and standard error.
    """

    working_df = df.copy() # Create a copy of the DataFrame to avoid modifying the original data
    x = working_df[x_name]
    y = working_df[y_name]

    slope,intercept,r_value,p_value,std_err = scipy.stats.linregress(x,y) # Perform linear regression
    R_squared = get_R_squared(x,y,slope,intercept) # Calculate R-squared value
    r = get_r(x,y,slope,intercept) # Calculate Pearson correlation coefficient (r)
    out_dict = {
        'slope': slope,
        'intercept': intercept,
        'r_value': r_value,
        'r': r,
        'r_squared':r**2,
        'R_squared': R_squared,
        'p_value': p_value,
        'std_err': std_err
    }
    return out_dict