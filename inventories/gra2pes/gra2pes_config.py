import numpy as np
import os

class Gra2pesConfig():
    # The following lines are those that should be changed to match the data you want to download/work with and 
    # the paths for your system. 
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    years = [2021] # Currently 2021, 2022, and 2023 are available for non-methane data.
    data_source = 'ftp' # 'ftp' or 'https'
    ftp_credentials_path = '/uufs/chpc.utah.edu/common/home/u0890904/credentials/ftp_gra2pes_credentials2.txt' # Path to the FTP credentials file
    parent_path = '/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES' # Parent path for where the data will be stored
    base_id = 'base_v1.0' # Base ID for the data, used in file naming and structure

    # Extra ID details for methane data, if applicable.
    # This is used to differentiate methane data from other GRA2PES data as it is a different 
    # product that is included separately. Currently only methane data for 2021 is available.
    extra_id_details = {'methane': {'folder_name':'v1_methane_beta',
                                    'years': [2021]}}

    ############################################################################################################
    # Everything below is used for the file structure and naming conventions and should generally not be changed.
    # as it matches the GRA2PES data structure.
    ############################################################################################################
    day_type_details = {'satdy':[5],'sundy':[6],'weekdy':[0,1,2,3,4]}
    day_types = list(day_type_details.keys())
    sector_details = {
        'AG' : {'description': 'Agriculture'},
        'AVIATION' : {'description': 'Aviation'},
        'COMM' : {'description': 'Commercial'},
        'COOKING' : {'description': 'Cooking'},
        'EGU' : {'description': 'Electricity Generation'},
        'FUG' : {'description': 'Fugitive'},
        'INDF' : {'description': 'Industrial fuel'},
        'INDP' : {'description': 'Industrial processes'},
        'INTERNATIONAL' : {'description': 'International'},
        'OFFROAD' : {'description': 'Off-road vehicles'},
        'OG' : {'description': 'Oil and gas'},
        'ONROAD_DSL' : {'description': 'On-road diesel'},
        'ONROAD_GAS' : {'description': 'On-road gasoline'},
        'RAIL' : {'description': 'Rail'},
        'RES' : {'description': 'Residential'},
        'SHIPPING' : {'description': 'Shipping'},
        'VCP' : {'description': 'VCP'},
        'WASTE' : {'description': 'Waste'},
        'total' : {'description': 'Total'}
    }
    sectors = list(sector_details.keys())

    base_path_structure = '{parent_path}/{base_id}'
    base_path = base_path_structure.format(parent_path=parent_path, base_id=base_id)
    base_fname_structure = '{year_str}{month_str}/{day_type}/GRA2PESv1.0_{sector}_{year_str}{month_str}_{day_type}_{hour_start}to{hour_end}Z.nc'

    regridded_path_structure = '{parent_path}/regridded{regrid_id}'
    regridded_day_subpath_structure = '{year_str}/{month_str}/{day_type}'
    regridded_fname_structure = '{sector}_regridded.nc'

    def __init__(self):
        pass

class Gra2pesRegridConfig():
    lat_spacing = 0.025
    lon_spacing = lat_spacing
    lat_center_range = (18.95, 58.05)
    lon_center_range = (-138.05, -58.95)
    method = 'conservative'
    input_dims=('south_north','west_east')
    weights_file = 'create'
    regrid_id = f'{lat_spacing}x{lon_spacing}'

    # Below is an example of how to set encoding details for the regridded data.
    # Uncomment and modify as needed for your specific use case.
    # encoding_details = {
    #     'zlib': True,              # Use zlib compression
    #     'complevel': 1,            # Compression level (1 is low, 9 is high)
    #     'shuffle': True,           # Use the shuffle filter to improve compression
    #     'chunksizes': ('utc_hour','bottom_top','lat','lon'),  # Set chunk shape to full size for lat lon
    # }

    def __init__(self,config):
        self.config = config
        self.regridded_path =  self.get_regridded_path()
        self.grid_out = self.get_grid_out()

    def get_grid_out(self):
        """Creates the grid_out dictionary for the regridding process"""

        grid_out = {
            'lat': np.arange(self.lat_center_range[0], self.lat_center_range[1], self.lat_spacing),  # Center Point Spacing Lat
            'lon': np.arange(self.lon_center_range[0], self.lon_center_range[1], self.lon_spacing),  # Center Point Spacing Lon
            'lat_b': np.arange(self.lat_center_range[0]-self.lat_spacing/2, self.lat_center_range[1]+self.lat_spacing/2, self.lat_spacing),  # Boundary Spacing Lat
            'lon_b': np.arange(self.lon_center_range[0]-self.lon_spacing/2, self.lon_center_range[1]+self.lon_spacing/2, self.lon_spacing),  # Boundary Spacing Lon
        }
        return grid_out
    
    def get_regridded_path(self):
        """Gets the regridded path for the regridded data using the config"""
        regridded_path = self.config.regridded_path_structure.format(parent_path=self.config.parent_path,regrid_id=self.regrid_id)
        return regridded_path