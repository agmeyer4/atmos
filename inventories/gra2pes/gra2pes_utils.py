import os
import warnings
import pyproj
import calendar
import datetime
import sys
import xesmf as xe
import numpy as np
import xarray as xr
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__),'../..'))
from utils import datetime_utils

# Suppress the specific FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning, message="The return type of `Dataset.dims` will be changed")

def set_ds_encoding(ds, encoding_details, vars_to_set = 'all'):
    """Set the encoding details for a dataset
    
    Args:
        ds (xr.Dataset) : the dataset to set the encoding details for
        encoding_details (dict) : the encoding details to set
    
    Returns:
        xr.Dataset : the dataset with the encoding details set
    """
    if vars_to_set == 'all':
        vars_to_set = ds.variables

    dim_sizes = ds.sizes
    # Resolve chunksizes
    resolved_chunksizes = tuple(
        dim_sizes[dim] if isinstance(dim, str) and dim in dim_sizes else dim for dim in encoding_details['chunksizes']
    )

    encoding = {}
    for var in ds.data_vars:
        encoding[var] = {
            'zlib': encoding_details['zlib'],
            'complevel': encoding_details['complevel'],
            'shuffle': encoding_details['shuffle'],
            'chunksizes': resolved_chunksizes,
        }
        
    return encoding

def get_daytype_from_int(day_int,config):
    """Get the day type from an integer
    
    Args:
        day_int (int) : the integer to get the day type from
        config (Gra2pesConfig) : the configuration object for the GRA2PES inventory
    
    Returns:
        str : the day type
    """
    for day_type,intlist in config.day_type_details.items():
        if day_int in intlist:
            return day_type
    raise ValueError(f"Day type {day_int} not found in config")

def get_inrange_list(dtr,config):
    '''Gets all unique year/month/daytype combinations in a datetime range
    
    Args:
        dtr (DateTimeRange) : the datetime range object from utils.datetime_utils
        config (Gra2pesConfig) : the config object
    
    Returns:
        unique_yr_mo_daytypes (list) : list of dictionaries with 'year', 'month', and 'day_type' keys
    '''
    
    dates_in_range = dtr.get_dates_in_range()
    yr_mo_daytypes_in_range = []
    for dates in dates_in_range:
        yr_mo_daytype = {}
        yr_mo_daytype['year'] = dates.year
        yr_mo_daytype['month'] = dates.month
        yr_mo_daytype['day_type'] = get_daytype_from_int(dates.weekday(),config)
        yr_mo_daytypes_in_range.append(yr_mo_daytype)

    unique_yr_mo_daytypes = [dict(t) for t in {tuple(d.items()) for d in yr_mo_daytypes_in_range}]
    unique_yr_mo_daytypes = sorted(unique_yr_mo_daytypes, key=lambda x: (x['year'], x['month'], x['day_type']))
    return unique_yr_mo_daytypes

class BaseGra2pesHandler():
    """ This class is meant to handle the file storage and naminv conventions for the "base" GRA2PES files, as downloaded
    and organized by gra2pes_base_creator.py. 
    
    Attributes:
        config (Gra2pesConfig) : the configuration object for the GRA2PES inventory
        base_path (str) : the base path to the GRA2PES files, as defined in the config
        specs (str or list) : the variables to load from the files. If 'all', all variables will be loaded
        extra_ids (list) : list of extra ids to load in addition to the main file, as of now only "methane" is applicable
    """

    def __init__(self, config, specs='all', extra_ids=None):
        self.config = config
        self.base_path = config.base_path
        self.specs = specs
        if extra_ids == None:
            self.extra_ids = []
        elif type(extra_ids) == list:
            self.extra_ids = extra_ids
        elif type(extra_ids) == str:
            self.extra_ids = [extra_ids]
        else:
            raise ValueError("extra_ids must be a list, string, or None")
    
    def load_fmt_fullday(self, sector, year, month, day_type, chunks = {}, check_extra = True):
        """Load the full day of data for a given sector, year, month, and day type
        
        Args:
            sector (str) : the sector to load
            year (int) : the year to load
            month (int) : the month to load
            day_type (str) : the day type to load
            chunks (dict) : dictionary of chunks to pass to xarray.open_dataset
            check_extra (bool) : whether to check the extra datasets against the main dataset ensuring the same varibles, coordinates, and attributes
        
        Returns:
            xr.Dataset : the full dataset for the given sector, year, month, and day type
        """

        ds_list = []
        for hour_start in ['00','12']: # Load the two half-day files
            ds = self.load_fmt_single(sector, year, month, day_type, hour_start, chunks, check_extra) 
            ds_list.append(ds)
        full_ds = xr.concat(ds_list, dim='utc_hour') # Concatenate the two half-day files
        full_ds = self.rename_zlevel(full_ds) # Rename the zlevel coordinate
        return full_ds

    def load_fmt_single(self, sector, year, month, day_type, hour_start, chunks, check_extra):
        """Load a single half-day file for a given sector, year, month, day type, and hour start, along with any extra files

        Args:
            sector (str) : the sector to load
            year (int) : the year to load
            month (int) : the month to load
            day_type (str) : the day type to load
            hour_start (str) : the hour start to load ('00' or '12')
            chunks (dict) : dictionary of chunks to pass to xarray.open_dataset
            check_extra (bool) : whether to check the extra datasets against the main dataset ensuring the same varibles, coordinates, and attributes

        Returns:
            xr.Dataset : the dataset for the given sector, year, month, day type, and hour start
        """

        # Get the relative file path (will be the same for both main and extra, with the addition of the extra id)
        relpath_fname = self.get_relpath_fname(sector, year, month, day_type, hour_start)

        # Load the main file
        main_full_fpath = os.path.join(self.base_path, relpath_fname)
        main_ds = xr.open_dataset(main_full_fpath, chunks=chunks)

        # Load the extra files
        extra_ds = {}
        for extra_id in self.extra_ids:
            extra_full_fpath = os.path.join(self.base_path,extra_id,relpath_fname)
            extra_ds = xr.open_dataset(extra_full_fpath, chunks=chunks)
            if check_extra:
                self.check_extra_against_main(main_ds, extra_ds)
            extra_vars = self.get_extra_vars(main_ds, extra_ds)
            main_ds = xr.merge([main_ds, extra_ds[extra_vars]])

        # Select the desired variables
        if self.specs != 'all':
            main_ds = main_ds[self.specs]

        # Change the time to utc_hour for clarity
        main_ds = self.change_time_to_utc_hour(main_ds)

        # Oranize and add some attributes
        main_ds.attrs['sector'] = sector
        main_ds.attrs['year'] = year
        main_ds.attrs['month'] = month
        main_ds.attrs['day_type'] = day_type 
        del main_ds.attrs['Sector']      
        
        return main_ds

    def get_relpath_fname(self, sector, year, month, day_type, hour_start):
        """Get the relative file path for a given sector, year, month, day type, and hour start. 

        The hour end will be inferred from the hour start, and the structure is defined in the config. This is the relative path 
        from the base path, so "main" files will be in the base directory and "extra" files will follow the same conventino except 
        from base_path/extra_id
        
        Args:
            sector (str) : the sector to load
            year (int) : the year to load
            month (int) : the month to load
            day_type (str) : the day type to load
            hour_start (str) : the hour start to load ('00' or '12')
        
        Returns:
            str : the relative file path
        """

        year_str = f'{year:04d}' # Convert year to string with 4 digits
        month_str = f'{month:02d}' # Convert month to string with 2 digits 
        if hour_start == '00': # Infer the hour end from the hour start
            hour_end = '11'
        elif hour_start == '12': 
            hour_end = '23'
        else:
            raise ValueError("hour_start must be '00' or '12'") 
        
        # Format the relative path file name using the information in the config
        relpath_fname = self.config.base_fname_structure.format(year_str=year_str, month_str=month_str, day_type=day_type, sector=sector, hour_start=hour_start, hour_end=hour_end)
        return relpath_fname
    
    def get_extra_vars(self, main_ds, extra_ds):
        """Get the extra variables in the extra dataset that are not in the main dataset
        
        Args:
            main_ds (xr.Dataset) : the main dataset
            extra_ds (xr.Dataset) : the extra dataset
        
        Returns:
            set : the set of extra variables in the extra dataset
        """

        extra_vars = set(extra_ds.variables) - set(main_ds.variables)
        return extra_vars

    def check_extra_against_main(self, main_ds, extra_ds):
        """Check that the extra dataset matches the main dataset in terms of variables, attributes, and coordinates, dimensions

        Args:
            main_ds (xr.Dataset) : the main dataset
            extra_ds (xr.Dataset) : the extra dataset

        Raises:
            AssertionError : if the extra dataset does not match the main dataset
        """

        attrs_equal = main_ds.attrs == extra_ds.attrs # Check that the attributes are the same
        dims_equal = main_ds.dims == extra_ds.dims # Check that the dimensions are the same
        coord_keys_equal = main_ds.coords.keys() == extra_ds.coords.keys() # Check that the coordinate keys are the same
        coord_values_equal = all([main_ds.coords[key].equals(extra_ds.coords[key]) for key in main_ds.coords.keys()]) # Check that the coordinate values are the same
        extra_vars = self.get_extra_vars(main_ds, extra_ds) # Get the extra variables
        shared_vars = set(main_ds.variables) & set(extra_ds.variables) # Get the shared variables
        xr.testing.assert_equal(main_ds[shared_vars], extra_ds[shared_vars]) # Check that the shared variables are equal

        # Raise an error if any of the checks fail
        assert attrs_equal and dims_equal and coord_keys_equal and coord_values_equal, f"Extra dataset does not match main dataset. Extra variables: {extra_vars}"
    
    def change_time_to_utc_hour(self, ds):
        """Change the time coordinate from a datetime on the first day of each month to utc_hour integer for clarity

        Args:
            ds (xr.Dataset) : the dataset to change the time coordinate of

        Returns:
            xr.Dataset : the dataset with the time coordinate changed to utc_hour as an int
        """

        ds['Time'] = ds['Time'].dt.hour
        ds = ds.rename({'Time':'utc_hour'})
        return ds

    def rename_zlevel(self,ds):
        """Rename the zlevel coordinate from bottom_top to zlevel
        
        Args:
            ds (xr.Dataset) : the dataset to rename the zlevel coordinate of
        
        Returns:
            xr.Dataset : the dataset with the bottom_top coordinate renamed to zlevel
        """

        ds = ds.rename({'bottom_top':'zlevel'})
        return ds

class Gra2pesRegridder():
    """This class is meant to handle the regridding of the "base" GRA2PES files to a new grid.
    
    Attributes:
        regrid_config (Gra2pesRegridConfig) : the regrid configuration object
        
    Methods:
        regrid : regrid a dataset
        create_regridder : create a regridder object
        create_ingrid : create the input grid for the regridder
        create_transformers : create the transformers for going from lambert conformal to WGS and vice versa
        proj4_from_ds : get a proj4 string defining a projection from a gra2pes "base" dataset
        save_regrid_weights : save the regridded weights to a file
    """

    def __init__(self, regrid_config):
        self.regrid_config = regrid_config #initialize with a regrid config object

    def regrid(self, ds):
        """Regrid a dataset according to the parameters of the class. Creates the regridder if it doesn't exist. 

        Args:
            ds (xr.Dataset) : the dataset to regrid

        Returns:
            xr.Dataset : the regridded dataset
        """

        try:  # Try to get the regridder
            self.regridder
        except: # If it doesn't exist, create it and save it to the class
            self.regridder = self.create_regridder(ds)
        
        #print('Regridding')
        regridded_ds = self.regridder(ds,keep_attrs = True) #regrid the dataset using the regridder
        old_attrs = list(regridded_ds.attrs.keys()) #get the old attributes 
        keep_attrs = ['sector','year','month','day_type','TITLE','regrid_method'] #attributes to keep

        #remove all attributes that aren't in the keep_attrs list
        for attr in old_attrs: 
            if attr not in keep_attrs:
                del regridded_ds.attrs[attr]
            else:
                continue

        return regridded_ds

    def create_regridder(self, ds, save_to_self = False):
        """Create a regridder object from a "base" gra2pes dataset

        Args:
            ds (xr.Dataset) : the xarray dataset created by the Gra2pesBaseHandler to use as the base for the regridder
            save_to_self (bool) : whether to save the regridder to the class

        Returns:
            xe.Regridder : the regridder object
        """

        print('Creating regridder')
        grid_in = self.create_ingrid(ds) #create the input grid
        grid_out = self.regrid_config.grid_out #get the output grid from the regrid config
        method = self.regrid_config.method 
        input_dims = self.regrid_config.input_dims

        regridder = xe.Regridder(grid_in, grid_out, method, input_dims = input_dims) #create the regridder

        regridder.ds_attrs = ds.attrs #save the attributes of the dataset to the regridder

        if save_to_self: 
            self.regridder = regridder

        return regridder

    def create_ingrid(self, ds):
        """Creates the grid that will be input into the regridder from a "base" gra2pes dataset. Adapted from Colin Harkins at NOAA
        
        Args:
            ds (xarray.DataSet) : the xarray dataset to use as the grid_in
        
        Returns:
            grid_in (dict) : a grid_in type dictionary that can be fed to xe.Regridder
        """

        #create the projection transformers. 'wgs_to_lcc' converts EPSG4326 to lambert conformal, 'lcc_to_wgs' does the opposite
        wgs_to_lcc, lcc_to_wgs = self.create_transformers(ds)  

        # Calculate the easting and northings of the domain center point
        e,n = wgs_to_lcc.transform(ds.CEN_LON, ds.CEN_LAT) #use the attribributes to transform lat lons defined in the ds as center to lcc

        # Grid parameters from the dataset
        dx, dy = ds.DX, ds.DY 
        nx, ny = ds.dims['west_east'], ds.dims['south_north']

        # bottom left corner of the domain
        x0 = -(nx-1) / 2. * dx + e
        y0 = -(ny-1) / 2. * dy + n

        # Calculating the boundary X-Y Coordinates
        x_b, y_b = np.meshgrid(np.arange(nx+1) * dx + x0 -dx/2, np.arange(ny+1) * dy + y0 -dy/2)
        x_bc, y_bc = lcc_to_wgs.transform(x_b, y_b)

        #define the input grid
        grid_in = {
            'lat':ds['XLAT'].values,
            'lon':ds['XLONG'].values,
            'lat_b':y_bc,
            'lon_b':x_bc
        }

        return grid_in

    def create_transformers(self, ds):
        """Create transformers for going from lambert conformal to WGS and vice versa
        
        Args:
            ds (xarray.dataset) : use the dataset to define the lcc projection, as gra2pes "base" data is in LCC
        
        Returns:
            wgs_to_lcc (pyproj.Transformer) : a transformer object to transform from WGS coordinates to LCC coordinates
            lcc_to_wgs (pyproj.Transformer) : a transformer object to transform from LCC coordinates to WGS coordinates
        """

        proj4_str = self.proj4_from_ds(ds) #get the proj4 string from the dataset
        lcc_crs = pyproj.CRS.from_proj4(proj4_str) #define the lcc coordinate reference system using the proj4 string
        wgs_crs = pyproj.CRS.from_epsg(4326) #define wgs coordinates as espg 4326
        wgs_to_lcc = pyproj.Transformer.from_crs(wgs_crs,lcc_crs,always_xy=True) #create one transformer
        lcc_to_wgs = pyproj.Transformer.from_crs(lcc_crs,wgs_crs,always_xy=True) #create the other transformer

        return wgs_to_lcc, lcc_to_wgs

    def proj4_from_ds(self,ds,map_proj = None, earth_rep = 'sphere'):
        """Get a proj4 string defining a projection from a gra2pes "base" dataset 
        
        Args:
            ds (xarray.Dataset) : a dataset to get the projection from
            map_proj (proj,optional) :the pyproj.Proj proj argument. allow the user to directly define the map proj, defaults to getting it from the dataset
            earth_rep (str) : change the earth representation of the input projection TODO, only can do "sphere" right now

        Returns:
            str : a proj4 string defining the projection
        """
         
        if map_proj == None: #if the user didn't define a map projection
            try: #try it so we can catch bad ones
                if ds.MAP_PROJ_CHAR == 'Lambert Conformal': #teh datasets from noaa_csl "base" data should have the "Lambert Conformal" attribute, so confirm this
                    map_proj = 'lcc' #if so, the map_proj code is lcc
                else: #if not, something unexpected happend
                    raise ValueError(f"Unknown map projection in ds.MAP_PROJ_CHAR: {ds.MAP_PROJ_CHAR}")
            except: #if it failed, something weird happened
                raise Exception('No map projection found')
            
        if earth_rep == 'sphere': #can only do spheres for now. this is how the noaa_csl "base" data is represented (like WRF) see references
            my_proj = pyproj.Proj(proj=map_proj, # projection type: Lambert Conformal Conic
                                lat_1=ds.TRUELAT1, lat_2=ds.TRUELAT2, # Cone intersects with the sphere
                                lat_0=ds.MOAD_CEN_LAT, lon_0=ds.STAND_LON, # Center point
                                a=6370000, b=6370000) # The Earth is a perfect sphere
        else:
            raise Exception("Haven't dealt with non spheres yet")
        
        return my_proj.to_proj4()

    def save_regrid_weights(self, save_path):
        """Saves the regridded weights to a file
        
        Args:
            save_path (str) : the path in which to store the weights
        """

        fname = f"regrid_weights.nc"
        self.regridder.to_netcdf(os.path.join(save_path,fname))
        pass

class RegriddedGra2pesHandler:
    """This class is meant to handle the regridded GRA2PES .nc files in a specific directory structure created using gra2pes_regrid.py
    
    Attributes:
        config (Gra2pesConfig) : the configuration object for the GRA2PES inventory
        regrid_id (str) : the regrid id for the regridded files
        regridded_path (str) : the path to the regridded files
        
    Methods:
        open_ds_inrange : open a dataset in a datetime range
        open_ds_single : open a single dataset
        rework_ds_dt : rework the datetime coordinates of a dataset
        get_files_inrange : get the files in a datetime range
        get_regridded_path : get the regridded path
        get_relpath_fname : get the relative path file name for a given sector, year, month, and day type
    """

    def __init__(self,config,regrid_id):
        self.config = config
        self.regrid_id = regrid_id
        self.regridded_path = self.get_regridded_path()

    def get_regridded_path(self):
        """Gets the regridded path for the regridded data using the config

        Returns:
            str : the regridded path

        Raise:
            ValueError : if the regridded path does not exist
        """

        regridded_path = self.config.regridded_path_structure.format(parent_path=self.config.parent_path,regrid_id=self.regrid_id)
        if not os.path.exists(regridded_path):
            raise ValueError(f"Regridded path {regridded_path} does not exist.")
        return regridded_path

    def get_files_inrange(self,dtr,sectors = 'all'):
        """Get the files in a datetime range, optionally for specific sectors

        Args:
            dtr (DateTimeRange) : the datetime range object from utils.datetime_utils
            sectors (str or list) : the sectors to get the files for. If 'all', all sectors will be used

        Returns:
            list : the list of files in the datetime range
        """

        if sectors == 'all':
            sectors = self.config.sectors
        inrange_list = get_inrange_list(dtr,self.config)  #get a list of dictionaries for the year, month, and day type representing the files we want
        files_inrange = []
        for yr_mo_daytype in inrange_list: # Loop through the years, months, and daytypes 
            for sector in sectors: #Loop through the sectors
                day_subpath = self.get_day_subpath(yr_mo_daytype['year'],yr_mo_daytype['month'],yr_mo_daytype['day_type']) #get the subpath to that day
                fname = self.config.regridded_fname_structure.format(sector=sector) #get the file name
                fullpath = os.path.join(self.regridded_path,day_subpath,fname) #define the full path
                files_inrange.append(fullpath) #add the full path to the list
        return sorted(files_inrange) #return the list sorted
    
    def get_day_subpath(self,year,month,day_type):
        """Get the day subpath for a given year, month, and day type

        Args:
            year (int) : the year
            month (int) : the month
            day_type (str) : the day type

        Returns:
            str : the day subpath, relative to the regridded_path
        """

        year_str = f'{year:04d}'
        month_str = f'{month:02d}'
        #use the config to build the subpath
        day_subpath = self.config.regridded_day_subpath_structure.format(year_str=year_str, month_str=month_str, day_type=day_type)
        return day_subpath
    
    def open_ds_inrange(self,dtr,sectors = 'all',chunks = {}):
        """Opens a dataset with values within the datetime range and optionally for specific sectors
        
        Args:
            dtr (DateTimeRange) : the datetime range object from utils.datetime_utils
            sectors (str or list) : the sectors to get the files for. If 'all', all sectors will be used
            
        Returns:
            xr.Dataset : the dataset with values in the datetime range, nicely organized as a regridded_dataset with sectors as dimensions 
        """

        files_inrange = self.get_files_inrange(dtr,sectors) #get the files in the datetime range
        ds_list = []
        for fname in files_inrange: 
            ds = self.open_ds_single(fname) #open each file
            ds_list.append(ds) #add it to the list
        ds_combined = xr.combine_by_coords(ds_list,combine_attrs='drop_conflicts') #combine the datasets, dropping the conflicting attributes
        ds_combined = ds_combined.transpose('lat','lon','year','month','day_type','utc_hour','sector') #transpose the dataset 
        #below is a little unecessary, but it orders the coordinates in a way that makes sense when printing in jupyter or elsewhere
        ds_combined = ds_combined.assign_coords(
            lat=ds_combined['lat'],
            lon=ds_combined['lon'],
            year=ds_combined['year'],
            month=ds_combined['month'],
            day_type=ds_combined['day_type'],
            utc_hour=ds_combined['utc_hour'],
            sector=ds_combined['sector']
        ) 
        return ds_combined

    def open_ds_single(self,fname,chunks = {}):
        """Open a single dataset from a file

        Args:
            fname (str) : the file name to open
            chunks (dict) : the chunks to pass to xarray.open_dataset

        Returns:
            xr.Dataset : the opened dataset
        """

        ds = xr.open_dataset(fname)
        #assign the coordinates based on the attributes that were logged in each dataset during the regrid. 
        ds = ds.assign_coords(year=ds.attrs['year'], month=ds.attrs['month'], day_type=ds.attrs['day_type'],sector=ds.attrs['sector']) 
        ds = ds.expand_dims(dim=['year','month','day_type','sector'])
        
        return ds

    def rework_ds_dt(self,ds):
        """Combines the weird datetime coordinates (year, month, day_type, utc_hour) into a single datetime coordinate
        
        Args:
            ds (xr.Dataset) : the dataset to rework the datetime coordinates of, should have dims ('year','month','day_type','utc_hour','sector')
            
        Returns:
            xr.Dataset : the dataset with a datetime coordinate/dimension that is just the actual datetime
        """

        combined_ds_list = [] #initialize a list to hold the datasets
        for year in ds.year.values: #loop through the years
            for month in ds.month.values: #loop through the months
                #get the dates in the month
                dt1 = datetime.datetime(year, month, 1)
                dt2 = datetime.datetime(year, month, calendar.monthrange(year, month)[1])
                dates_in_month = datetime_utils.DateTimeRange(dt1,dt2,tz='UTC').get_dates_in_range()

                #find the dates that are in each day type
                dates_by_day_type = {
                    'weekdy':[date for date in dates_in_month if date.weekday() in self.config.day_type_details['weekdy']],
                    'satdy':[date for date in dates_in_month if date.weekday() in self.config.day_type_details['satdy']],
                    'sundy':[date for date in dates_in_month if date.weekday()  in self.config.day_type_details['sundy']]
                }        

                #select the current month from the ds
                month_ds = ds.sel(year = year, month = month) 

                new_month_ds_list = [] #create a list to hold the new datasets for this month
                for day_type,dates in dates_by_day_type.items(): #loop through the day types 
                    subds = month_ds.sel(day_type=day_type).drop_vars(['year','month','day_type']) #select the current day type 
                    subds = subds.assign_coords({'date':dates}) #assign the date coordinate using the list of dates for that day type

                    #create a list of datetimes for the new datetime coordinate
                    datetimes = [pd.Timestamp(date) + pd.Timedelta(hours=int(hour)) for date in subds.coords['date'].values for hour in subds.coords['utc_hour'].values]
                    datetime_index = pd.DatetimeIndex(datetimes) #create a datetime index from the list of datetimes

                    subds = subds.stack({'datetime':('date','utc_hour')}) # stack the date and utc_hour into a datetime coordinate
                    subds = subds.drop_vars(['date','utc_hour','datetime']) #drop the old date and utc_hour coordinates
                    subds = subds.assign_coords({'datetime':datetime_index}) #assign the new datetime coordinate
                    new_month_ds_list.append(subds) #add the new dataset to the list

                #concatenate the datasets for the current month
                new_month_ds = xr.concat(new_month_ds_list,dim='datetime').sortby('datetime')

                #add the new month dataset to the list
                combined_ds_list.append(new_month_ds)

        #concatenate the datasets for all the months        
        combined_ds = xr.concat(combined_ds_list,dim='datetime').sortby('datetime')

        return combined_ds