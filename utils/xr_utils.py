import xarray as xr


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


def trim_ds_to_extent(ds,extent,lat_name='lat',lon_name='lon'):
    """Trim the dataset to the given extent."""
    out_ds = ds.sel(**{lat_name: slice(extent['lat_min'], extent['lat_max']),
                       lon_name: slice(extent['lon_min'], extent['lon_max'])})
    return out_ds