import xarray as xr

# Define Functions
def trim_ds_to_extent(ds,extent,lat_name='lat',lon_name='lon'):
    """Trim the dataset to the given extent."""
    out_ds = ds.sel(**{lat_name: slice(extent['lat_min'], extent['lat_max']),
                       lon_name: slice(extent['lon_min'], extent['lon_max'])})
    return out_ds

# Define classes
class UnitConverter:
    def __init__(self):
        self.conversion_funcs = {
            ('km^-2', 'm^-2'): lambda data: data * 1E-6,
            ('g', 'metric_Ton'): lambda data: data * 1E-6,
            ('umole', 'mole'): lambda data: data * 1E-6,
            ('s^-1', 'h^-1'): lambda data: data * 3600,
        }

    def register_conversion(self, from_unit, to_unit, func):
        """Register a new unit conversion function."""
        self.conversion_funcs[(from_unit, to_unit)] = func

    def update_all_units(self, ds, old_unit, new_unit):
        """Update unit metadata for all variables in the dataset using update_units."""
        for var in ds.data_vars:
            updated_var = self.update_units(ds[var], old_unit, new_unit)  # Get modified DataArray
            ds[var] = updated_var  # Reassign modified DataArray to dataset
        return ds

    def update_units(self, var, old_unit, new_unit):
        """Update unit metadata for a single variable and return the modified DataArray."""
        if 'units' in var.attrs and old_unit in var.attrs['units']:
            var = var.copy()  # Ensure we don't modify the original reference unexpectedly
            var.attrs['units'] = var.attrs['units'].replace(old_unit, new_unit)
        return var  # Return modified DataArray

    def convert(self, ds, from_unit, to_unit):
        """Perform an in-place conversion for all variables with the given units."""
        if (from_unit, to_unit) not in self.conversion_funcs:
            raise ValueError(f"Conversion from {from_unit} to {to_unit} not defined.")

        conversion_func = self.conversion_funcs[(from_unit, to_unit)]

        for var in ds.data_vars:
            if 'units' in ds[var].attrs and from_unit in ds[var].attrs['units']:
                ds[var].data *= conversion_func(1)  # Apply conversion in-place
                ds[var] = self.update_units(ds[var], from_unit, to_unit)

        return ds  # Returning ds for convenience, but it is modified in-place

    def convert_mole_to_g(self, ds, molar_masses):
        """Convert mole to grams using provided molar masses in-place."""
        for var in ds.data_vars:
            if 'units' in ds[var].attrs and 'mole' in ds[var].attrs['units']:
                molar_mass = molar_masses.get(var)
                if molar_mass is None:
                    raise ValueError(f"Molar mass for variable {var} not provided.")

                ds[var].data *= molar_mass  # Modify data in-place
                ds[var] = self.update_units(ds[var], 'mole', 'g')
            if 'units' in ds[var].attrs and 'mol' in ds[var].attrs['units']:
                molar_mass = molar_masses.get(var)
                if molar_mass is None:
                    raise ValueError(f"Molar mass for variable {var} not provided.")

                ds[var].data *= molar_mass  # Modify data in-place
                ds[var] = self.update_units(ds[var], 'mol', 'g')
        return ds

    def convert_m2_to_gridcell(self, ds, gca):
        """Convert from per square meter to per grid cell and return a modified copy."""
        ds_copy = ds.copy()  # Create a new dataset to avoid modifying the original

        for var in ds_copy.data_vars:
            if 'units' in ds_copy[var].attrs and 'm^-2' in ds_copy[var].attrs['units']:
                orig_attrs = ds_copy[var].attrs  # Save attributes
                ds_copy[var] = ds_copy[var] * gca  # Apply multiplication
                ds_copy[var].attrs = orig_attrs  # Restore attributes
                ds_copy[var] = self.update_units(ds_copy[var], 'm^-2', 'gridcell^-1')

        return ds_copy  # Return the modified dataset

class InventoryRatios():
    def __init__(self, ds,molar_masses):
        self.ds = ds
        self.molar_masses = molar_masses

    def get_total_ratio(self, num, denom):
        self.check_ratioability(num, denom)
        mmu = self.get_molemass_unit(num)
        if 'gridcell^-1' not in self.ds[num].units or 'gridcell^-1' not in self.ds[denom].units:
            raise ValueError(f"{num} and {denom} must have units of gridcell^-1 to sum over gridcells")
        if mmu == 'mole':
            return float(self.ds[num].sum() / self.ds[denom].sum())
        elif (mmu == 'g') | (mmu == 'metric_Ton'):
            return float(self.ds[num].sum() / self.ds[denom].sum() * self.molar_masses[denom] / self.molar_masses[num])
    
    def get_gc_ratio(self,num,denom,num_quantile_filter = None, denom_quantile_filter = None):
        self.check_ratioability(num, denom)
        mmu = self.get_molemass_unit(num)

        if num_quantile_filter is not None:
            ds_num = self.ds[num].where(self.ds[num] > self.ds[num].quantile(num_quantile_filter))
        else:
            ds_num = self.ds[num]
        if denom_quantile_filter is not None:
            ds_denom = self.ds[denom].where(self.ds[denom] > self.ds[denom].quantile(denom_quantile_filter))
        else:
            ds_denom = self.ds[denom]

        ratio_id = f"{num.lower()}_{denom.lower()}"
        if mmu == 'mole':
            ratio = ds_num / ds_denom
        elif (mmu == 'g') | (mmu == 'metric_Ton'):
            ratio = ds_num / ds_denom * self.molar_masses[denom] / self.molar_masses[num]

        # Calculate denomnum_eq_sum
        if num == 'CH4':
            denomnum_eq_sum = ds_num * 84 + ds_denom
        elif num == 'CO':
            denomnum_eq_sum = ds_num * 1.57 + ds_denom
        else:
            denomnum_eq_sum = ds_num + ds_denom

        # Create a new dataset with the ratio and denomnum_eq_sum
        new_ds = xr.Dataset({
            ratio_id: ratio,
            f'{denom}_eq_sum': denomnum_eq_sum
        })

        return new_ds
        
    def check_ratioability(self, num, denom):
        if num not in list(self.ds.data_vars):
            raise ValueError(f"{num} is not in the dataset")
        if denom not in list(self.ds.data_vars):
            raise ValueError(f"{denom} is not in the dataset")
        if self.ds[num].units != self.ds[denom].units:
            raise ValueError(f"Units of {num} and {denom} do not match")
        
    def get_molemass_unit(self, var):
        mmu = self.ds[var].units.split()[0]
        if (mmu == 'g') & (var not in self.molar_masses.keys()):
            raise ValueError(f"{var} is in grams but a molar mass is not defined for this var")
        
        return mmu

