""" 

"""

#Import packages
import sys
import time
import os
import pickle
from configs.gra2pes import gra2pes_config
from utils import gen_utils, gra2pes_utils, datetime_utils
from utils.xr_utils import slice_extent

def load_slice_retime_save():
    """
    """
    pass

def main():
    """
    """
    t1 = time.time() #Start the timer

    # ------------------------------------------------------------------
    # Load unified config (from YAML via the Gra2pesConfig class)
    # ------------------------------------------------------------------
    config = gra2pes_config.Gra2pesConfig()
    regrid_config = gra2pes_config.Gra2pesRegridConfig(config)
    slice_retime_config = gra2pes_config.Gra2pesSliceRetimeConfig(config)

    # Create the regridded path if it doesn't exist
    if not os.path.exists(slice_retime_config.output_path):
        os.makedirs(slice_retime_config.output_path)
    else:
        raise Exception(f'Output path {slice_retime_config.output_path} already exists, please choose a new output path to avoid overwriting existing data.')

    # Pull out top-level configs
    slices = slice_retime_config.slices
    start_dt = slice_retime_config.start_dt
    end_dt = slice_retime_config.end_dt
    timezone = slice_retime_config.timezone

    #Create the datetimerange
    dtr = datetime_utils.DateTimeRange(slice_retime_config.start_dt,slice_retime_config.end_dt,tz = slice_retime_config.timezone)


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
        print('Pre processes: ', pre_processes)
    if post_processes:
        print('Post processes: ', post_processes)
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
        f.write(f'Pre processes: {pre_processes}\n')
        f.write(f'Post processes: {post_processes}\n')
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
