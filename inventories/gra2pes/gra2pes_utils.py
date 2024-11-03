import xarray as xr
import os
import warnings
import pyproj
import xesmf as xe
import numpy as np

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

    def __init__(self, regrid_config):
        self.regrid_config = regrid_config

    def regrid(self, ds):
        try: 
            self.regridder
        except:
            self.regridder = self.create_regridder(ds)
        
        print('Regridding')
        regridded_ds = self.regridder(ds,keep_attrs = True)
        old_attrs = list(regridded_ds.attrs.keys())
        keep_attrs = ['sector','year','month','day_type','TITLE','regrid_method']
        for attr in old_attrs:
            if attr not in keep_attrs:
                del regridded_ds.attrs[attr]
            else:
                continue
        return regridded_ds

    def create_regridder(self, ds, save_to_self = False):
        print('Creating regridder')
        grid_in = self.create_ingrid(ds)
        grid_out = self.regrid_config.grid_out
        method = self.regrid_config.method
        input_dims = self.regrid_config.input_dims
        regridder = xe.Regridder(grid_in, grid_out, method, input_dims = input_dims)
        regridder.ds_attrs = ds.attrs
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

    def save_regrid_weights(self, regridder, save_path = None, fname = None):
        pass


