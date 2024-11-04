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
    os.makedirs(regrid_subpath,exist_ok=True)        
    return regrid_subpath

def load_regrid_save(BGH,gra2pes_regridder,sector,year,month,day_type,pre_processes=None,post_processes=None):
    '''Regrid the gra2pes data and save it to the regridded path
    ''' 
    day_regrid_path = create_regrid_subpath(gra2pes_regridder.regrid_config,year,month,day_type)
    save_fname = f'{sector}_regridded.nc'
    full_save_path = os.path.join(day_regrid_path,save_fname)
    if os.path.exists(os.path.join(day_regrid_path,save_fname)):
        raise ValueError(f"Regridded dataset {full_save_path} already exists, you may end up overwriting data")

    base_ds = BGH.load_fmt_fullday(sector,year,month,day_type)

    if pre_processes:
        for func,params in pre_processes:
            base_ds = func(base_ds,**params)

    regridded_ds = gra2pes_regridder.regrid(base_ds)
    regridded_ds.attrs['git_hash'] = gen_utils.get_githash()

    if post_processes:
        for func,params in post_processes:
            regridded_ds = func(regridded_ds,**params)
    
    print('Loading regridded dataset into memory')
    regridded_ds.load()
    print('Saving regridded dataset')
    regridded_ds.to_netcdf(full_save_path)
    return

def sum_on_dim(ds,**kwargs):
    '''Sum the dataset on a dimension
    '''
    dim = kwargs['dim']
    print('Summing on dimension ',dim)
    if dim == 'zlevel':
        ds = ds.sum(dim=dim,keep_attrs = True)
    else:
        ds = ds.sum(dim=dim)
    return ds

def slice_extent(ds,**kwargs):
    extent = kwargs['extent']
    print('Slicing extent: ',extent)
    ds = ds.sel(lat=slice(extent['lat_min'], extent['lat_max']),
                          lon=slice(extent['lon_min'], extent['lon_max']))
    return ds


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
    print('')

    extra_ids = 'methane'
    specs = ['CO2','CO','HC01']#,'HC02','HC14','NH3','NOX','SO2']
    BGH = gra2pes_utils.BaseGra2pesHandler(config,specs = specs, extra_ids = extra_ids)

    sectors = config.sector_details.keys()
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    years = [2021]
    day_types = ['satdy','sundy','weekdy']

    pre_sum_dim = 'zlevel'
    extent = {'lon_min': -113, 'lon_max': -111, 'lat_min': 40, 'lat_max': 42}
    
    pre_processes = [(sum_on_dim,{'dim':pre_sum_dim})]
    post_processes = [(slice_extent,{'extent':extent})]

    gra2pes_regridder = gra2pes_utils.Gra2pesRegridder(regrid_config)

    for year in years:
        for month in months:
            for day_type in day_types:
                for sector in sectors:
                    print(f'Regridding {sector} for {year}-{month} {day_type}')
                    gen_utils.check_space(regrid_config.regridded_path)
                    try:
                        load_regrid_save(BGH,gra2pes_regridder,sector,year,month,day_type,pre_processes=pre_processes,post_processes=post_processes)
                    except Exception as e:
                        print(f'Error at {time.time()}')
                        raise Exception(e)
                    print('')

    t2 = time.time()
    print(f'Finished regrid at {t2}')
    print(f'Time taken: {t2-t1} seconds')
    return

if __name__ == "__main__":
    main()
