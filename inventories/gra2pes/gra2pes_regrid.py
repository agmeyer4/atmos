import sys
import time
import os
import gra2pes_utils 
import gra2pes_config
sys.path.append(os.path.join(os.path.dirname(__file__),'../..'))
from utils import gen_utils

def create_regrid_subpath(regrid_config,year,month,day_type):
    '''Create the subpath for the regridded data
    '''
    regrid_subpath = os.path.join(regrid_config.regridded_path,f'{year:04d}',f'{month:02d}',day_type)
    if not os.path.exists(regrid_subpath):
        os.makedirs(regrid_subpath)
    else:
        raise ValueError(f"Regridded subpath {regrid_subpath} already exists, you may end up overwriting data")
    return regrid_subpath

def regrid_and_save(BGH,gra2pes_regridder,sector,year,month,day_type):
    '''Regrid the gra2pes data and save it to the regridded path
    ''' 
    day_regrid_path = create_regrid_subpath(gra2pes_regridder.regrid_config,year,month,day_type)
    base_ds = BGH.load_fmt_fullday(sector,year,month,day_type)
    regridded_ds = gra2pes_regridder.regrid(base_ds)
    regridded_ds.attrs['git_hash'] = gen_utils.get_githash()
    
    save_fname = f'{sector}_regridded.nc'
    print('Saving regridded dataset')
    regridded_ds.to_netcdf(os.path.join(day_regrid_path,save_fname))
    return

def main():
    print('Regridding GRA2PES data from base (Lambert Conical Conformal) to regridded (Lat/Lon)')
    t1 = time.time()
    print(f'Git hash of this regrid = {gen_utils.get_githash()}')
    print(f'Starting regrid at {t1}')

    config = gra2pes_config.Gra2pesConfig()
    regrid_config = gra2pes_config.Gra2pesRegridConfig()
    if not os.path.exists(regrid_config.regridded_path):
        os.makedirs(regrid_config.regridded_path)
    print(f'Saving regridded data to {regrid_config.regridded_path}')

    extra_ids = 'methane'
    specs = ['CO2','CO','HC01','HC02','HC14','NH3','NOX','SO2']
    BGH = gra2pes_utils.BaseGra2pesHandler(config,specs = specs, extra_ids = extra_ids)
    sectors = config.sector_details.keys()
    months = [6]
    years = [2021]
    day_types = ['weekdy']

    gra2pes_regridder = gra2pes_utils.Gra2pesRegridder(regrid_config)

    for year in years:
        for month in months:
            for day_type in day_types:
                for sector in sectors:
                    print(f'Regridding {sector} for {year}-{month} {day_type}')
                    try:
                        regrid_and_save(BGH,gra2pes_regridder,sector,year,month,day_type)
                    except Exception as e:
                        print(f'Error at {time.time()}')
                        raise Exception(e)
                    print('')

    t2 = time.time()
    print(f'Finished regrid at {t2}')
    print(f'Time taken: {t2-t1}')
    return

if __name__ == "__main__":
    main()
