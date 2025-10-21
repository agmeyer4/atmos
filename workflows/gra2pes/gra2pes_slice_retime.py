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

def open_slice_retime(rgh, dtr, full_gca, extent):
    """
    
    """

    #get the encoding details
    

    # Load the regridded dataset for the given date range and spatial extent
    ds = rgh.open_ds_inrange(dtr, slice_extent=extent)

    # Slice the grid cell area to the given extent
    sliced_gca = slice_extent(full_gca, extent)

    # Retime the dataset to hourly resolution
    ds = rgh.rework_ds_dt(ds)

    # Add the sliced grid cell area to the dataset
    ds['grid_cell_area'] = sliced_gca

    return ds

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

    #Create the datetimerange
    dtr = datetime_utils.DateTimeRange(slice_retime_config.start_dt,slice_retime_config.end_dt,tz = slice_retime_config.timezone)

    # Create the regrid handler:
    rgh = gra2pes_utils.RegriddedGra2pesHandler(regrid_config) 

    # Get the grid cell area 
    full_gca = rgh.get_full_gca()

    # Loop through the slices and process each one
    for slice_id, extent in slice_retime_config.slices.items():
        if slice_id != 'SaltLakeCity':
            continue
        output_fullpath = os.path.join(slice_retime_config.output_path,f'{slice_id}.nc')
        print(f'Processing slice {slice_id} with extent {extent}. Saving to {output_fullpath}.')

        # Load, retime, and save the sliced dataset
        ds = open_slice_retime(rgh, dtr, full_gca, extent)

        # Ensure the dataset is chunked appropriately for saving
        ds = ds.chunk(slice_retime_config.encoding_details['chunksizes'])

        # Set the encoding for the dataset
        encoding = gra2pes_utils.set_ds_encoding(ds, slice_retime_config.encoding_details)

        # Save the dataset to netCDF
        ds.to_netcdf(output_fullpath, encoding=encoding)

    #Stop the timer and print some final stuff
    t2 = time.time()
    print(f'Finished slice/retime at {t2}')
    print(f'Time taken: {t2-t1} seconds')
    return

if __name__ == "__main__":
    main()
