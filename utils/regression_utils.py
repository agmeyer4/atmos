"""Regression utilities for atmospheric data analysis."""

# Imports
import numpy as np
import scipy
from pylr2 import regress2

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


def york(xi, yi, dxi, dyi, ri=0.0, b0=1.0, maxIter=1e6):
    """Make a linear bivariate fit to xi, yi data using York et al. (2004).
    This is an implementation of the line fitting algorithm presented in:
    York, D et al., Unified equations for the slope, intercept, and standard
    errors of the best straight line, American Journal of Physics, 2004, 72,
    3, 367-375, doi = 10.1119/1.1632486
    See especially Section III and Table I. The enumerated steps below are
    citations to Section III
    Parameters:
      xi, yi      x and y data points
      dxi, dyi    errors for the data points xi, yi
      ri          correlation coefficient for the weights
      b0          initial guess b
      maxIter     float, maximum allowed number of iterations
    Returns:
      a           y-intercept, y = a + bx
      b           slope
      S           goodness-of-fit estimate
      sigma_a     standard error of a
      sigma_b     standard error of b
    Usage:
    [a, b] = bivariate_fit( xi, yi, dxi, dyi, ri, b0, maxIter)

    https://gist.github.com/mikkopitkanen/ce9cd22645a9e93b6ca48ba32a3c85d0
    """
    # (1) Choose an approximate initial value of b
    b = b0

    # (2) Determine the weights wxi, wyi, for each point.
    wxi = 1.0 / dxi**2.0
    wyi = 1.0 / dyi**2.0

    alphai = (wxi * wyi)**0.5
    b_diff = 999.0

    # tolerance for the fit, when b changes by less than tol for two
    # consecutive iterations, fit is considered found
    tol = 1.0e-8

    # iterate until b changes less than tol
    iIter = 1
    while (abs(b_diff) >= tol) & (iIter <= maxIter):

        b_prev = b

        # (3) Use these weights wxi, wyi to evaluate Wi for each point.
        Wi = (wxi * wyi) / (wxi + b**2.0 * wyi - 2.0*b*ri*alphai)

        # (4) Use the observed points (xi ,yi) and Wi to calculate x_bar and
        # y_bar, from which Ui and Vi , and hence betai can be evaluated for
        # each point
        x_bar = np.sum(Wi * xi) / np.sum(Wi)
        y_bar = np.sum(Wi * yi) / np.sum(Wi)

        Ui = xi - x_bar
        Vi = yi - y_bar

        betai = Wi * (Ui / wyi + b*Vi / wxi - (b*Ui + Vi) * ri / alphai)

        # (5) Use Wi, Ui, Vi, and betai to calculate an improved estimate of b
        b = np.sum(Wi * betai * Vi) / np.sum(Wi * betai * Ui)

        # (6) Use the new b and repeat steps (3), (4), and (5) until successive
        # estimates of b agree within some desired tolerance tol
        b_diff = b - b_prev

        iIter += 1

    # (7) From this final value of b, together with the final x_bar and y_bar,
    # calculate a from
    a = y_bar - b * x_bar

    # Goodness of fit
    S = np.sum(Wi * (yi - b*xi - a)**2.0)

    # (8) For each point (xi, yi), calculate the adjusted values xi_adj
    xi_adj = x_bar + betai

    # (9) Use xi_adj, together with Wi, to calculate xi_adj_bar and thence ui
    xi_adj_bar = np.sum(Wi * xi_adj) / np.sum(Wi)
    ui = xi_adj - xi_adj_bar

    # (10) From Wi , xi_adj_bar and ui, calculate sigma_b, and then sigma_a
    # (the standard uncertainties of the fitted parameters)
    sigma_b = np.sqrt(1.0 / np.sum(Wi * ui**2))
    sigma_a = np.sqrt(1.0 / np.sum(Wi) + xi_adj_bar**2 * sigma_b**2)

    # calculate covariance matrix of b and a (York et al., Section II)
    cov = -xi_adj_bar * sigma_b**2
    # [[var(b), cov], [cov, var(a)]]
    cov_matrix = np.array(
        [[sigma_b**2, cov], [cov, sigma_a**2]])

    if iIter <= maxIter:
        return a, b, S, cov_matrix, sigma_a, sigma_b
    else:
        print("bivariate_fit.py exceeded maximum number of iterations, " +
              "maxIter = {:}".format(maxIter))
        return np.nan, np.nan, np.nan, np.nan, np.nan, np.nan
    

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

def get_r(x,y):
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
    r = get_r(x,y) # Calculate Pearson correlation coefficient (r)
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


def odr_regression(df,x_name, y_name, x_err_name, y_err_name,model_kwargs = {},data_kwargs = {},odr_kwargs = {},run_kwargs={},out_keychange = {}):
    x = df[x_name]
    x_err = df[x_err_name]
    y = df[y_name]
    y_err = df[y_err_name]

    model = scipy.odr.Model(linear_model,**model_kwargs)

    data = scipy.odr.RealData(x,y,x_err,y_err,**data_kwargs)

    myodr = scipy.odr.ODR(data,model,**odr_kwargs)
    myoutput = myodr.run(**run_kwargs)
    myoutput_dict = myoutput.__dict__
    out_dict = {
        'slope': myoutput_dict['beta'][0],
        'intercept': myoutput_dict['beta'][1],
        'sd_slope': myoutput_dict['sd_beta'][0],
        'sd_intercept': myoutput_dict['sd_beta'][1],
    }
    R_squared = get_R_squared(x,y,out_dict['slope'],out_dict['intercept'])
    r = get_r(x,y)
    out_dict['r'] = r
    out_dict['r_squared'] = r**2
    out_dict['R_squared'] = R_squared
    for key, value in myoutput_dict.items():
        if key not in out_keychange.keys():
            out_dict[key] = value 
        else:
            out_dict[out_keychange[key]] = value
            
    return out_dict

def york_regression(df,x_name, y_name, x_err_name, y_err_name, york_kwargs = {}):
    working_df = df.copy()
    
    x = working_df[x_name]
    x_err = working_df[x_err_name]
    y = working_df[y_name]
    y_err = working_df[y_err_name]

    intercept, slope, S, cov, se_intercept, se_slope = york(x,y,x_err,y_err,**york_kwargs)
    R_squared = get_R_squared(x,y,slope,intercept)
    r = get_r(x,y)
    out_dict = {
        'slope': slope,
        'intercept': intercept,
        'r': r,
        'r_squared': r**2,
        'R_squared': R_squared,
        'S': S,
        'cov': cov,
        'se_slope': se_slope,
        'se_intercept': se_intercept
    }
    return out_dict

def rma_regression(df,x_name,y_name):
    working_df = df.copy()
    x = working_df[x_name]
    y = working_df[y_name]
    results = regress2(x,y,_method_type_2 = 'reduced major axis')
    R_squared = get_R_squared(x,y,results['slope'],results['intercept'])
    r = get_r(x,y)
    out_dict = {
        'slope': results['slope'],
        'intercept': results['intercept'],
        'r_value': results['r'],
        'r': r,
        'r_squared': r**2,
        'R_squared': R_squared,
        'sd_slope': results['std_slope'],
        'sd_intercept': results['std_intercept']
    }
    return out_dict

def calculate_all_regressions(df,x_name,y_name,err_tags):
    all_regression_outputs = {}
    for err_tag in err_tags:
        x_err_name = f'{x_name.split("_")[0]}_{err_tag}'
        y_err_name = f'{y_name.split("_")[0]}_{err_tag}'

        regr_outs = {
            'details': {'x_name': x_name, 'y_name': y_name, 'x_err_name': x_err_name, 'y_err_name': y_err_name},
            'ols': ols_regression(df, x_name, y_name),
            'odr': odr_regression(df, x_name, y_name, x_err_name, y_err_name, odr_kwargs={'beta0': (0.007, 0)}, out_keychange={'res_var': 'chi_squared'}),
            'york': york_regression(df, x_name, y_name, x_err_name, y_err_name),
            'rma': rma_regression(df, x_name, y_name)
        }
        all_regression_outputs[err_tag] = regr_outs
    return all_regression_outputs