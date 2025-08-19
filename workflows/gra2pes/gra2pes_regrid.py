""" This script regrids the gra2pes data from the base (Lambert Conical Conformal) to the regridded (Lat/Lon) grid

There are two places you need to set up configurations for this script:
gra2pes_config.py -- these are the general configurations both for the base data and the regridded data, file path structures, etc. Ensure parameters in both
                    Gra2pesConfig and Gra2pesRegridConfig are set correctly for your system.

main() -- in this script, this is where the specifics of the regrid are set up. This includes the sectors, years, months, day_types, specs, extra_ids, 
          pre_processes, post_processes, etc. All of the editable parameters are on the top. 

Run the script directly either using python gra2pes_regrid.py, or using slurm with the slurm script slurm/gra2pes/gra2pes_regrid.sbatch 

"""

#Import packages
import sys
import time
import os
import pickle
from configs.gra2pes import gra2pes_config
from utils import gen_utils, gra2pes_utils

def create_regrid_subpath(regrid_config,year,month,day_type):
    """Create the subpath for the regridded data
    
    Args:
        regrid_config (Gra2pesRegridConfig): The regrid configuration
        year (int): The year of the data
        month (int): The month of the data
        day_type (str): The day type of the data (satdy, sundy or weekdy)
    
    Returns:
        str: The full path to the regridded data
    """

    #Get the path to the day type folder using the regrid config subpath structure
    regrid_day_relpath = regrid_config.config.regridded_day_subpath_structure.format(year_str = f'{year:04d}', month_str = f'{month:02d}', day_type = day_type)
    regrid_subpath = os.path.join(regrid_config.regridded_path,regrid_day_relpath) #Join the regridded path with the day type folder
    os.makedirs(regrid_subpath,exist_ok=True) #Make the day type folder if it doesn't exist        
    return regrid_subpath

def load_regrid_save(BGH,gra2pes_regridder,sector,year,month,day_type,pre_processes=None,post_processes=None):
    """Script to load the base data, regrid it, and save it according to parameter and pre/post processes
    
    Args:
        BGH (BaseGra2pesHandler): The base gra2pes handler
        gra2pes_regridder (Gra2pesRegridder): The regridder
        sector (str): sector id
        year (int): year
        month (int): month
        day_type (str): day type (satdy, sundy, weekdy)
        pre_processes (list, optional): List of tuples of functions and keyword parameters to apply to base_ds before regridding. Defaults to None.
        post_processes (list, optional): List of tuples of functions and keyword parameters to apply to regridded_ds after regridding. Defaults to None.

    Returns:
        xarray.Dataset: The regridded dataset

    Raises:
        ValueError: If the regridded dataset already exists, this prevents against overwriting data if it has already been regridded and saved

    """

    day_regrid_path = create_regrid_subpath(gra2pes_regridder.regrid_config,year,month,day_type) #Create the regrid subpath
    save_fname = f'{sector}_regridded.nc' #Create the save file name
    full_save_path = os.path.join(day_regrid_path,save_fname) 
    if os.path.exists(os.path.join(day_regrid_path,save_fname)): 
        raise ValueError(f"Regridded dataset {full_save_path} already exists, you may end up overwriting data")

    base_ds = BGH.load_fmt_fullday(sector,year,month,day_type,check_extra=False) #Load the base dataset

    if pre_processes: #Apply pre processes
        for func,params in pre_processes:
            base_ds = func(base_ds,**params)

    regridded_ds = gra2pes_regridder.regrid(base_ds) #Regrid the dataset
    regridded_ds.attrs['git_hash'] = gen_utils.get_githash() #Add the git hash to the attributes

    if post_processes: #Apply post processes
        for func,params in post_processes:
            regridded_ds = func(regridded_ds,**params)
    
    print('Loading regridded dataset into memory')
    regridded_ds.load() #Load the dataset into memory for easier writing 

    #Set the encoding for the dataset
    if gra2pes_regridder.regrid_config.encoding_details:
        encoding = gra2pes_utils.set_ds_encoding(regridded_ds, gra2pes_regridder.regrid_config.encoding_details) #Set the encoding for the dataset
    else:
        encoding = None

    print('Saving regridded dataset') 
    regridded_ds.to_netcdf(full_save_path,encoding = encoding) #Save the regridded dataset
    return regridded_ds

def sum_on_dim(ds,**kwargs):
    """Sum the dataset on a dimension
    
    A typical preprocess step to reduce the dimensionality of the dataset before the regrid
    
    Args:
        ds (xarray.Dataset): The dataset
        **kwargs: dim (str): The dimension to sum on
        
    Returns:
        xarray.Dataset: The dataset summed on the dimension
    """

    dim = kwargs['dim']
    if dim == 'zlevel': # Should probably handle this case by case for the different dimensions. For zlevel, the attributes don't matter as much for the actual species. This may be a problem later
        ds = ds.sum(dim=dim,keep_attrs = True)
    else:
        ds = ds.sum(dim=dim)
    return ds

def slice_extent(ds,**kwargs):
    """Slice the dataset to a specific extent
    
    A typical postprocess step to reduce the extent of the dataset after the regrid
    
    Args:
        ds (xarray.Dataset): The dataset
        **kwargs: extent (dict): The extent to slice to
        
    Returns:
        xarray.Dataset: The dataset sliced to the extent
    """

    extent = kwargs['extent']
    ds = ds.sel(lat=slice(extent['lat_min'], extent['lat_max']),
                lon=slice(extent['lon_min'], extent['lon_max']))
    return ds

def main():
    """Main function to regrid the gra2pes data

    This function sets up the configurations for the regrid, and then loops through the sectors, years, months, and day types to regrid the data.
    The regrid is saved to the regridded path, and details about the regrid are saved to the details folder.
    
    To use, set your parameters in this main function at the top of the script, and then run the script either directly or with the slurm script.
    """
    t1 = time.time() #Start the timer

    #Data parameters (editable)
    extra_ids = 'methane' # This is an extra id that is not in the base data, but is in another folder which we want to include in the regrid
    specs = ['CO2','CO','HC01','HC02','HC14','NH3','NOX','SO2'] #These are the species we want to regrid
    sectors = 'all' #The sectors to include in the 
    months = [1,2,3,4,5,6,7,8,9,10,11,12] #The months to include in the regrid
    years = [2021] #The years to include in the regrid
    day_types = ['satdy','sundy','weekdy'] #The day types to include in the regrid

    #Processing parameters (editable)
    pre_sum_dim = 'zlevel' #The dimension to sum on before the regrid (inputs to sum_on_dim)
    extent = {'lon_min': -113, 'lon_max': -111, 'lat_min': 40, 'lat_max': 42} #The extent to slice to after the regrid (inputs to slice_extent)
    pre_processes = [(sum_on_dim,{'dim':pre_sum_dim})] #List of preprocesses to apply to the base data before the regrid 
    post_processes = None#[(slice_extent,{'extent':extent})] #List of postprocesses to apply to the regridded data after the regrid

    #Set up the configurations and create the regridded path
    config = gra2pes_config.Gra2pesConfig()
    regrid_config = gra2pes_config.Gra2pesRegridConfig(config)
    if not os.path.exists(regrid_config.regridded_path):
        os.makedirs(regrid_config.regridded_path)
    if sectors == 'all':
        sectors = config.sectors

    #Create the base gra2pes handler and the regridder
    BGH = gra2pes_utils.BaseGra2pesHandler(config,specs = specs, extra_ids = extra_ids) 
    gra2pes_regridder = gra2pes_utils.Gra2pesRegridder(regrid_config)

    #Print a bunch of stuff to the console
    print('Regridding GRA2PES data from base (Lambert Conical Conformal) to regridded (Lat/Lon)')
    print(f'Git hash of this regrid = {gen_utils.get_githash()}')
    print(f'Starting regrid at {t1}')
    print(f'Saving regridded data to {regrid_config.regridded_path}')
    print(f'Years: {years}')
    print(f'Months: {months}')
    print(f'Day types: {day_types}')
    print(f'Sectors: {sectors}')
    print(f'Specs: {specs}')
    print(f'Extra ids: {extra_ids}')
    if pre_processes:
        print('Pre processes: ','sum_on_dim ',pre_sum_dim)
    if post_processes:
        print('Post processes: ','slice_extent ',extent)
    print('\n')

    #Loop through the sectors, years, months, and day types to regrid the data
    for year in years:
        for month in months:
            for day_type in day_types:
                for sector in sectors:
                    print(f'Regridding {sector} for {year}-{month} {day_type}')
                    gen_utils.check_space(regrid_config.regridded_path)
                    try:
                        regridded_ds = load_regrid_save(BGH,gra2pes_regridder,sector,year,month,day_type,pre_processes=pre_processes,post_processes=post_processes)
                    except Exception as e:
                        print(f'Error at {time.time()}')
                        raise Exception(e)
                    print('')

    #Create a folder to hold details about the regrid and other files
    details_path = os.path.join(regrid_config.regridded_path,'details') #Create the details path
    os.makedirs(details_path,exist_ok=True) #Make the details path if it doesn't exist

    #Get some final stuff to put in the regrid details
    print('Creating example ds for grid cell area')
    base_ds = BGH.load_fmt_fullday(sectors[0],years[0],months[0],day_types[0]) #Load the very first base dataset
    regridded_ds = gra2pes_regridder.regrid(base_ds) #Regrid it
    example_ds = regridded_ds.isel(utc_hour = 0).drop_vars('utc_hour')[list(regridded_ds.data_vars.keys())[0]] #Pare it all the way down to just lat lon and one species
    example_ds.to_netcdf(os.path.join(details_path,'grid_out.nc')) #Save the example ds
    os.system(f'cdo gridarea {os.path.join(details_path,"grid_out.nc")} {os.path.join(details_path,"grid_out_area.nc")}') #create the grid area file from the example using cdo

    #Save the regrid details
    print(f'\nSaving regrid details to {details_path}') 
    with open(os.path.join(details_path,'regrid_details.txt'),'w') as f: #Write the details to a text file
        f.write(f'Git hash of this regrid = {gen_utils.get_githash()}\n')
        f.write(f'Years: {years}\n')
        f.write(f'Months: {months}\n')
        f.write(f'Day types: {day_types}\n')
        f.write(f'Sectors: {sectors}\n')
        f.write(f'Specs: {specs}\n')
        f.write(f'Extra ids: {extra_ids}\n')
        f.write(f'Pre processes: sum_on_dim {pre_sum_dim}\n')
        f.write(f'Post processes: slice_extent {extent}\n')
    with open(os.path.join(details_path,'regrid_config.pkl'),'wb') as f:  #Save the regrid config to a pickle file
        pickle.dump(regrid_config,f)
    gra2pes_regridder.save_regrid_weights(details_path) #Save the regrid weights to the details path

    #Stop the timer and print some final stuff
    t2 = time.time()
    print(f'Finished regrid at {t2}')
    print(f'Time taken: {t2-t1} seconds')
    return

if __name__ == "__main__":
    main()
