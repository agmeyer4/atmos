import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.patches as mpatches
import matplotlib
import os
import pandas as pd
import numpy as np
import sys
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.img_tiles as cimgt
sys.path.append('.')
from utils import regression_utils, gen_utils

#Declare functions
def plot_da_on_map(da,**kwargs):
    if 'map_extent' in kwargs.keys():
        map_extent = kwargs['map_extent']
    else:
        map_extent = {'lat_min':da.lat.min().values,'lat_max':da.lat.max().values,
                      'lon_min':da.lon.min().values,'lon_max':da.lon.max().values}
    if 'pcolormesh_kwargs' in kwargs.keys():
        pcolormesh_kwargs = kwargs['pcolormesh_kwargs']
    else:
        pcolormesh_kwargs = {}
    proj = ccrs.PlateCarree()
    fig = plt.figure(figsize=(10,10))
    ax = plt.axes(projection = proj)
    ax.set_extent([map_extent['lon_min'],map_extent['lon_max'],map_extent['lat_min'],map_extent['lat_max']],crs=proj)

    request = cimgt.GoogleTiles(style='street')
    scale = 10.0 # prob have to adjust this
    ax.add_image(request,int(scale))

    da.plot.pcolormesh('lon','lat',ax = ax,**pcolormesh_kwargs)
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS)
    ax.add_feature(cfeature.STATES)
    

def add_single_regression_to_ax(ax,df,x_name,y_name,regression_output,
                           x_err_name = None, y_err_name = None,
                           summary_keys = None,scatter_kwargs = {'s':5,'c':'k','zorder':5},regr_err = None):
    
    working_df = df.copy()
    x = working_df[x_name]
    y = working_df[y_name]
    x_err = working_df[x_err_name] if x_err_name else None
    y_err = working_df[y_err_name] if y_err_name else None

    x_line = np.array([x.min(), x.max()])
    y_line = regression_utils.linear_model([regression_output['slope'], regression_output['intercept']], x_line)

    ax.scatter(x,y,**scatter_kwargs)

    if x_err is not None and y_err is not None:
        ax.errorbar(x,y,xerr=x_err,yerr=y_err,fmt='o',markersize=0,c='grey',zorder = 4)

    ax.plot(x_line,y_line,c='r',zorder = 6)


    if summary_keys:
        summary_text = '\n'.join([f"{key}: {format(regression_output[key], f'.{3}e')}" for key in summary_keys])
        ax.plot([],[],c = 'red',label=summary_text)
    
    ax.legend()
    ax.set_xlabel(x_name)
    ax.set_ylabel(y_name)
    return ax

def truncate_colormap(cmap, minval=0.0, maxval=1.0, n=100):
    new_cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
        'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
        cmap(np.linspace(minval, maxval, n)))
    return new_cmap

def force_to_ylims(arr, ylims):
    arr = np.where(arr < ylims[0], ylims[0], arr)
    arr = np.where(arr > ylims[1], ylims[1], arr)
    return arr

def ratio_ts_plot(ax,rolling_regr_df,regr_params,regr_label,regr_type,labsize,markersize,markerlw,ylims=None):
    working_df = rolling_regr_df.copy()
    permil = regr_params['regr_labels'][regr_label]['permil']

    if ylims is not None:
        if permil:
            working_df[f'{regr_label}_{regr_type}_slope'] = force_to_ylims(working_df[f'{regr_label}_{regr_type}_slope'], [ylim/1000 for ylim in ylims])

    filtered_df = working_df.loc[working_df[f'{regr_label}_pass_filter']]
    grouped_df = working_df.loc[working_df[f'{regr_label}_good_group'].notnull()]

    if permil:
        scatter = ax.scatter(
                working_df.index,
                working_df[f'{regr_label}_{regr_type}_slope']*1000,
                s=markersize,
                c=working_df[f'{regr_label}_{regr_type}_r_squared'],
                linewidth = markerlw,
                vmin=0,vmax=1,
                cmap = 'Greys',
                edgecolors = 'grey'
            ) 
        scatter = ax.scatter(
                filtered_df.index,
                filtered_df[f'{regr_label}_{regr_type}_slope']*1000,
                s=markersize,
                c=filtered_df[f'{regr_label}_{regr_type}_r_squared'],
                vmin=0,vmax=1,
                linewidth = markerlw,
                cmap = 'Greys',
                edgecolors='blue'
            ) 
        scatter = ax.scatter(
                grouped_df.index,
                grouped_df[f'{regr_label}_{regr_type}_slope']*1000,
                s=markersize,
                c=grouped_df[f'{regr_label}_{regr_type}_r_squared'],
                vmin=0,vmax=1,
                linewidth = markerlw,
                cmap = 'Greys',
                edgecolors='limegreen'
            )         
    else:
        scatter = ax.scatter(
                working_df.index,
                working_df[f'{regr_label}_{regr_type}_slope'],
                s=markersize,
                c=working_df[f'{regr_label}_{regr_type}_r_squared'],
                linewidth = markerlw,
                vmin=0,vmax=1,
                cmap = 'Greys',
                edgecolors = 'grey'
            ) 
        scatter = ax.scatter(
                filtered_df.index,
                filtered_df[f'{regr_label}_{regr_type}_slope'],
                s=markersize,
                c=filtered_df[f'{regr_label}_{regr_type}_r_squared'],
                vmin=0,vmax=1,
                linewidth = markerlw,
                cmap = 'Greys',
                edgecolors='blue'
            ) 
        scatter = ax.scatter(
                grouped_df.index,
                grouped_df[f'{regr_label}_{regr_type}_slope'],
                s=markersize,
                c=grouped_df[f'{regr_label}_{regr_type}_r_squared'],
                vmin=0,vmax=1,
                linewidth = markerlw,
                cmap = 'Greys',
                edgecolors='limegreen'
            )      
        

    ax.tick_params(labelsize=labsize)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    if ylims is not None:
        ax.set_ylim(ylims)

    return scatter

def plot_stacked_seasonal_bars(
    df_grouped,
    var,
    fig_id,
    fig_path,
    palette,
    sector_order,
    savefig=False,
    showfig=True,
    labsize=44,
    legfontsize=44
):
    season_order = ['DJF', 'MAM', 'JJA', 'SON']

    df_grouped['Season'] = pd.Categorical(df_grouped['Season'], categories=season_order, ordered=True)
    color_dict = dict(zip(sector_order, palette))

    var_data = df_grouped[df_grouped['Variable'] == var]
    stacked_data = var_data.pivot_table(index='Season', columns='Sector', values='Sum', aggfunc='sum', observed=False)

    stacked_data = stacked_data.reindex(season_order)
    stacked_data = stacked_data[sector_order]

    # ---------- Main Plot ----------
    fig, ax = plt.subplots(figsize=(20, 8))
    bars = stacked_data.plot(
        kind='bar',
        stacked=True,
        ax=ax,
        color=[color_dict[sec] for sec in sector_order],
        width=0.5
    )

    max_value = stacked_data.sum(axis=1).max()
    exp = gen_utils.calculate_exponent(max_value)
    scale = 10 ** exp
    ax.yaxis.set_major_formatter(make_sci_formatter(scale, decimals=1))

    ax.set_xlabel('')
    ax.set_ylabel(f'{var} Emissions\n(Tonne × $10^{{{exp}}}$)', fontsize=labsize)
    ax.tick_params(axis='both', which='major', labelsize=labsize)
    ax.set_title('')
    ax.get_legend().remove()

    fig.tight_layout()

    if savefig:
        fig.savefig(os.path.join(fig_path, f"{fig_id}.png"), dpi=500, bbox_inches="tight")
    if showfig:
        plt.show()
    else:
        plt.close()

    # ----- Legend-Only Figure (Reversed) -----
    fig_leg = plt.figure(figsize=(6, 2))
    ax_leg = fig_leg.add_subplot(111)

    reversed_order = list(reversed(sector_order))
    handles = [plt.Rectangle((0, 0), 1, 1, color=color_dict[sec]) for sec in reversed_order]
    ax_leg.legend(handles, reversed_order, fontsize=legfontsize, loc='center', frameon=False,labelspacing=2.0)
    ax_leg.axis('off')

    fig_leg.tight_layout()

    if savefig:
        fig_leg.savefig(os.path.join(fig_path, f"{fig_id}_legend.png"), dpi=300, bbox_inches='tight')
    if showfig:
        plt.show()
    else:
        plt.close()




def make_sci_formatter(scale_factor, decimals=1):
    """Return a FuncFormatter that divides values by scale_factor and formats to the given number of decimal places."""
    def formatter(x, pos):
        return f'{x / scale_factor:.{decimals}f}'
    return FuncFormatter(formatter)


def bulk_regression_plotter(plotter,plot_df,regression_outputs,regr_label,err_tag,regr_type,
fig_id,savefig = False,showfig = False,annotate = True,legend_style = {},plot_kwargs = {},title=None):
    if annotate:
        fig_id = f'{fig_id}_ann'
    
    x_name = regression_outputs[regr_label][err_tag]['details']['x_name']
    y_name = regression_outputs[regr_label][err_tag]['details']['y_name']
    x_err_name = regression_outputs[regr_label][err_tag]['details']['x_err_name']
    y_err_name = regression_outputs[regr_label][err_tag]['details']['y_err_name']
    regression_output = regression_outputs[regr_label][err_tag][regr_type]

    # Plot using the 'co_co2' label
    fig = plotter.plot(
        plot_df, x_name, y_name, x_err_name, y_err_name,
        regression_output=regression_output,
        fig_id=fig_id,
        regr_label=regr_label,  
        savefig=savefig,
        showfig=showfig,
        legend_style = legend_style,
        title = title,
        **plot_kwargs
    )

# Declare Plotting Classes
class RegressionPlotter:
    def __init__(self, figures_path=".", regr_plot_dict=None):
        """
        Initializes the RegressionPlotter with default styles and figure path.
        """
        self.figures_path = figures_path
        self.regr_plot_dict = regr_plot_dict or {}

        # Default styles
        self.figsize = (5, 5)
        self.labsize = 22
        self.legend_style = {"fontsize": self.labsize}

    def set_style(self, **kwargs):
        """
        Updates plot styles dynamically, merging new values with the default ones.
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                if isinstance(value, dict):
                    current_style = getattr(self, key)
                    if isinstance(current_style, dict):
                        current_style.update(value)
                        setattr(self, key, current_style)
                    else:
                        setattr(self, key, value)
                else:
                    setattr(self, key, value)

    def update_plot_style(self, regr_label, **kwargs):
        """
        Updates the plotting style for a given regression label dynamically.
        
        Parameters:
        - regr_label (str): The regression type key to update.
        - kwargs: Dictionary of keys and values to update within the style.
        """
        if regr_label in self.regr_plot_dict:
            for key, value in kwargs.items():
                if key in self.regr_plot_dict[regr_label]:
                    if isinstance(self.regr_plot_dict[regr_label][key], dict) and isinstance(value, dict):
                        self.regr_plot_dict[regr_label][key].update(value)
                    else:
                        self.regr_plot_dict[regr_label][key] = value
                else:
                    print(f"Warning: {key} not found in {regr_label}, adding new entry.")
                    self.regr_plot_dict[regr_label][key] = value
        else:
            print(f"Error: Regression label '{regr_label}' not found in regr_plot_dict.")

    def plot(
        self, df, x_name, y_name, x_err_name=None, y_err_name=None, 
        regression_output=None, fig_id=None, regr_label=None, savefig=False, showfig=True,
        x_label=None, y_label=None, xlim=None, ylim=None, line_xlim=None, legend_style=None, title=None,
    ):
        """
        Plots a regression result with error bars and a best-fit line.
        The scatter, error bars, and regression line styles are determined by regr_plot_dict.
        """
        fig, ax = plt.subplots(1, 1, figsize=self.figsize)

        # Get the regression label settings from regr_plot_dict
        label_info = self.regr_plot_dict.get(regr_label, {})
        x_label = x_label or label_info.get('x_label', x_name)
        y_label = y_label or label_info.get('y_label', y_name)
        line_label_parts = label_info.get('line_label', [])
        annotate = label_info.get('annotate', True)
        label_position = label_info.get('label_position', 'best')
        permil = label_info.get('permil', False)

        # Get the styles from regr_plot_dict, with defaults
        scatter_style = label_info.get('scatter_style', {"s": 5, "c": "k", "zorder": 2})
        errorbar_style = label_info.get('errorbar_style', {"fmt": "o", "markersize": 0, "c": "grey", "zorder": 1, "linestyle": "none"})
        linestyle = label_info.get('linestyle', {"c": "k", "linewidth": 2})

        if legend_style is None:
            legend_style = {'fontsize': self.labsize}
        else:
            ls2 = {'fontsize': self.labsize}
            ls2.update(legend_style)
            legend_style = ls2

        # Error bars
        ax.errorbar(
            df[x_name], df[y_name],
            xerr=df[x_err_name] if x_err_name else None,
            yerr=df[y_err_name] if y_err_name else None,
            **errorbar_style,
        )

        # Scatter points
        ax.scatter(df[x_name], df[y_name], **scatter_style)

        # Regression line
        if regression_output:
            slope = regression_output["slope"]
            if permil:
                slope_label = slope * 1000
            else:
                slope_label = slope

            # Data bounds
            x_min, x_max = df[x_name].min(), df[x_name].max()
            y_min, y_max = df[y_name].min(), df[y_name].max()

            # Set a y-buffer (e.g., extend y-range by 10%)
            y_buffer = (y_max - y_min) * 0.1
            y_min_adj = y_min - y_buffer
            y_max_adj = y_max + y_buffer

            # Compute corresponding x-values based on y-limits using y = mx + b
            x_min_adj = max(x_min, (y_min_adj - regression_output["intercept"]) / slope)
            x_max_adj = min(x_max, (y_max_adj - regression_output["intercept"]) / slope)

            # Create the adjusted regression line
            xline = np.array([x_min_adj, x_max_adj])
            yline = slope * xline + regression_output["intercept"]


            # Safely format the line label parts with regression output
            try:
                formatted_parts = [part.format(slope=slope_label, r_squared=regression_output["r_squared"]) if needs_formatting else part for needs_formatting, part in line_label_parts]
                line_label = ''.join(formatted_parts)
            except KeyError as e:
                print(f"Key error: Missing {e} in regression_output. Using default label.")
                line_label = "Regression Line"

            # Plot the regression line
            ax.plot(xline, yline, **linestyle)  # Draw the line without adding it to the legend

            # Create a text-only legend entry
            text_patch = mpatches.Patch(color="none", label=line_label)  # Invisible patch

            # Add annotation if required
            if annotate:
                legend = ax.legend(
                handles=[text_patch], 
                handletextpad=0,  # Reduce space between marker and text
                handlelength=0,  # Remove space for the line
                **legend_style,
                )
                
        # Axis labels
        ax.set_xlabel(x_label, size=self.labsize)
        ax.set_ylabel(y_label, size=self.labsize)
        ax.tick_params(labelsize=self.labsize)

        # Title
        if title:
            ax.set_title(title, size=self.labsize)

        # Set axis limits if provided
        if xlim:
            ax.set_xlim(xlim)
        if ylim:
            ax.set_ylim(ylim)

        # Save figure
        if savefig:
            fig_name = f"{fig_id}.png"
            fig.savefig(os.path.join(self.figures_path, fig_name), dpi=500, bbox_inches="tight")

        # Show figure
        if showfig:
            plt.show()
        else:
            plt.close()
        
        return fig
        
    def plot_seasonal_details(
        self,
        seasonal_rolling_regr_details,
        regr_label,
        regr_type,
        fig_id=None,
        x_label=None,
        y_label=None,
        savefig=False,
        showfig=True,
        ylims=None,
        grapes_season_ratios=None,
        remove_x_ticks=False,
        season_positions=None,
        xlims=None
    ):
        """
        Plots the detailed statistics for each season and regression label.

        Parameters:
        - seasonal_rolling_regr_details (dict): The dictionary containing detailed statistics.
        - regr_label (str): The regression label to plot.
        - regr_type (str): The regression type to plot.
        - fig_id (str): The figure ID for saving the plot.
        - x_label (str): The label for the x-axis.
        - y_label (str): The label for the y-axis.
        - savefig (bool): Whether to save the figure.
        - showfig (bool): Whether to show the figure.
        - ylims (list): The y-axis limits.
        - grapes_season_ratios (dict): The dictionary containing inventory ratios.
        - season_positions (dict): Mapping of season names to specific x-axis positions.
        - xlims (list): Limits for the x-axis.
        """

        # Get the regression label settings from regr_plot_dict
        label_info = self.regr_plot_dict.get(regr_label, {})
        x_label = x_label or label_info.get('x_label', 'Season')
        y_label = y_label or label_info.get('y_label', 'Slope Value')
        permil = label_info.get('permil', False)

        # Get the styles from regr_plot_dict, with defaults
        errorbar_style = label_info.get('errorbar_style', {"fmt": "o", "markersize": 0, "c": "grey", "zorder": 1, "linestyle": "none"})
        inventory_style = label_info.get('inventory_style', {"fmt": "s", "markersize": 5, "c": "red", "zorder": 3})
        scatter_style = label_info.get('scatter_style', {"s": 5, "c": "k", "zorder": 2})
        
        slope_column = f'{regr_label}_{regr_type}_slope'
        
        season_x = []
        mean_values = []
        std_values = []
        inventory_values = []
        
        for season, details in seasonal_rolling_regr_details.items():
            if regr_label in details and season in season_positions:
                stats = details[regr_label]
                mean_value = stats.loc['mean', slope_column]
                std_value = stats.loc['std', slope_column]

                season_x.append(season_positions[season])
                mean_values.append(mean_value)
                std_values.append(std_value)
                
                if grapes_season_ratios and season in grapes_season_ratios:
                    inventory_values.append(grapes_season_ratios[season][regr_label])

        fig, ax = plt.subplots(1, 1, figsize=self.figsize)

        if permil:
            mean_values = [value * 1000 for value in mean_values]
            std_values = [value * 1000 for value in std_values]
            inventory_values = [value * 1000 for value in inventory_values]

        # Scatter plot for mean values
        ax.scatter(season_x, mean_values, **scatter_style)

        # Plot the mean and standard deviation of the slopes
        ax.errorbar(season_x, mean_values, yerr=std_values, **errorbar_style)

        # Plot the inventory ratios
        if inventory_values:
            ax.scatter(season_x, inventory_values, **inventory_style)

        # Set y-axis limits
        if ylims is not None:
            ax.set_ylim(ylims)

        # Set x-axis limits
        if xlims is not None:
            ax.set_xlim(xlims)

        # Set labels and title
        ax.set_xlabel(x_label, size=self.labsize)
        ax.set_ylabel(y_label, size=self.labsize)
        ax.tick_params(labelsize=self.labsize)

        # Remove x-ticks if specified
        if remove_x_ticks:
            ax.set_xticks([])

        # Save figure
        if savefig:
            fig_name = f"{fig_id}.png"
            fig.savefig(os.path.join(self.figures_path, fig_name), dpi=500, bbox_inches="tight")

        # Show figure
        if showfig:
            plt.show()
        else:
            plt.close()

# Rolling Regression Plotter
class RollingRegressionPlotter:
    def __init__(self, figures_path=".", rolling_regr_plot_dict=None):
        """
        Initializes the RollingRegressionPlotter with default styles and figure path.
        """
        self.figures_path = figures_path
        self.rolling_regr_plot_dict = rolling_regr_plot_dict or {}

        # Default styles
        self.figsize = (5, 5)
        self.labsize = 22
        self.legend_style = {"fontsize": self.labsize}

    def set_style(self, **kwargs):
        """
        Updates plot styles dynamically, merging new values with the default ones.
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                if isinstance(value, dict):
                    current_style = getattr(self, key)
                    if isinstance(current_style, dict):
                        current_style.update(value)
                        setattr(self, key, current_style)
                    else:
                        setattr(self, key, value)
                else:
                    setattr(self, key, value)

    def update_plot_style(self, regr_label, **kwargs):
        """
        Updates the plotting style for a given regression label dynamically.
        
        Parameters:
        - regr_label (str): The regression type key to update.
        - kwargs: Dictionary of keys and values to update within the style.
        """
        if regr_label in self.rolling_regr_plot_dict:
            for key, value in kwargs.items():
                if key in self.rolling_regr_plot_dict[regr_label]:
                    if isinstance(self.rolling_regr_plot_dict[regr_label][key], dict) and isinstance(value, dict):
                        self.rolling_regr_plot_dict[regr_label][key].update(value)
                    else:
                        self.rolling_regr_plot_dict[regr_label][key] = value
                else:
                    print(f"Warning: {key} not found in {regr_label}, adding new entry.")
                    self.rolling_regr_plot_dict[regr_label][key] = value
        else:
            print(f"Error: Regression label '{regr_label}' not found in rolling_regr_plot_dict.")

    def get_group_rolling_regression_details(self, df, regr_label, regr_type, x_name):
        """
        Computes regression lines for each time-based group using York regression slope & intercept.

        Parameters:
            df (pd.DataFrame): DataFrame with 'group' column and York regression results.
            regr_label (str): Regression label to extract slope and intercept columns.

        Returns:
            list of dict: Each entry contains 'x', 'y', 'group' for plotting.
        """
        lines = []
        
        for group, group_df in df.groupby(f'{regr_label}_good_group'):
            if len(group_df) < 2:
                continue  # Skip small groups
            
            # Compute mean slope & intercept for the group
            slope = group_df[f'{regr_label}_{regr_type}_slope'].mean()
            intercept = group_df[f'{regr_label}_{regr_type}_intercept'].mean()

            # Define x range
            x_min, x_max = group_df[x_name].min(), group_df[x_name].max()
            x_values = np.linspace(x_min, x_max, 100)

            # Compute y values using y = mx + b
            y_values = slope * x_values + intercept

            # Store results
            lines.append({'x': x_values, 'y': y_values, 'group': group})

        return lines

    def plot(
        self, df, regr_label, regr_type, x_name, y_name, x_err_name=None, y_err_name=None, 
        fig_id=None, savefig=False, showfig=True, x_label=None, y_label=None, xlim=None, ylim=None, line_xlim=None, legend_style=None,title=None
    ):
        """
        Plots the rolling regression results with error bars, scatter points, and regression lines.
        The scatter, error bars, and regression line styles are determined by rolling_regr_plot_dict.
        """
        fig, ax = plt.subplots(1, 1, figsize=self.figsize)

        # Get the regression label settings from rolling_regr_plot_dict
        label_info = self.rolling_regr_plot_dict.get(regr_label, {})
        x_label = x_label or label_info.get('x_label', x_name)
        y_label = y_label or label_info.get('y_label', y_name)
        permil = label_info.get('permil', False)

        # Get the styles from rolling_regr_plot_dict, with defaults
        scatter_style = label_info.get('scatter_style', {"s": 5, "c": "k", "zorder": 2})
        errorbar_style = label_info.get('errorbar_style', {"fmt": "o", "markersize": 0, "c": "grey", "zorder": 1, "linestyle": "none"})
        linestyle = label_info.get('linestyle', {"c": "k", "linewidth": 2})

        # Filter the DataFrame to include only good groups
        good_df = df.loc[df[f'{regr_label}_good_group'].notnull()]

        # Get grouped regression lines
        grouped_regression_lines = self.get_group_rolling_regression_details(df, regr_label, regr_type, x_name)

        # Plot error bars
        ax.errorbar(
            good_df[x_name],
            good_df[y_name],
            xerr=good_df[x_err_name] if x_err_name else None,
            yerr=good_df[y_err_name] if y_err_name else None,
            **errorbar_style
        )

        # Plot scatter points
        ax.scatter(
            good_df[x_name],
            good_df[y_name],
            **scatter_style
        )

        # Plot regression lines
        for line in grouped_regression_lines:
            ax.plot(line['x'], line['y'], **linestyle)

        # Axis labels
        ax.set_xlabel(x_label, size=self.labsize)
        ax.set_ylabel(y_label, size=self.labsize)
        ax.tick_params(labelsize=self.labsize)

        # Title
        if title:
            ax.set_title(title, size=self.labsize)

        # Set axis limits if provided
        if xlim:
            ax.set_xlim(xlim)
        if ylim:
            ax.set_ylim(ylim)

        # Save figure
        if savefig:
            fig_name = f"{fig_id}.png"
            fig.savefig(os.path.join(self.figures_path, fig_name), dpi=500, bbox_inches="tight")

        # Show figure
        if showfig:
            plt.show()
        else:
            plt.close()
        
        return fig
