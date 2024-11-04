import numpy as np

class Gra2pesConfig():
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    years = [2021]
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

    base_path = '/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES/base_v1.0'
    base_fname_structure = '{year_str}{month_str}/{day_type}/GRA2PESv1.0_{sector}_{year_str}{month_str}_{day_type}_{hour_start}to{hour_end}Z.nc'

    regridded_parent_path = '/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES'
    regridded_path_structure = '{regridded_parent_path}/regridded{regrid_id}'
    regridded_fname_structure = '{year_str}/{month_str}/{day_type}/{sector}_regridded.nc'

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
    regridded_path =  f'/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES/regridded{regrid_id}'
    # encoding_details = {
    #     'zlib': True,              # Use zlib compression
    #     'complevel': 1,            # Compression level (1 is low, 9 is high)
    #     'shuffle': True,           # Use the shuffle filter to improve compression
    #     'chunksizes': ('utc_hour','bottom_top','lat','lon'),  # Set chunk shape to full size for lat lon
    # }

    def __init__(self):
        self.grid_out = self.get_grid_out()

    def get_grid_out(self):
        grid_out = {
            'lat': np.arange(self.lat_center_range[0], self.lat_center_range[1], self.lat_spacing),  # Center Point Spacing Lat
            'lon': np.arange(self.lon_center_range[0], self.lon_center_range[1], self.lon_spacing),  # Center Point Spacing Lon
            'lat_b': np.arange(self.lat_center_range[0]-self.lat_spacing/2, self.lat_center_range[1]+self.lat_spacing/2, self.lat_spacing),  # Boundary Spacing Lat
            'lon_b': np.arange(self.lon_center_range[0]-self.lon_spacing/2, self.lon_center_range[1]+self.lon_spacing/2, self.lon_spacing),  # Boundary Spacing Lon
        }
        return grid_out