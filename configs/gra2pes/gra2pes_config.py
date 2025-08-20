"""
------------------------------------------------------------------------------
GRA2PES Configuration Classes

This module defines Python classes for accessing and interpreting
configuration settings for the GRA2PES emission workflow and its
regridding step.

Configuration is stored in a single YAML file, which is parsed into:
- Gra2pesConfig: general data-handling settings and path structure
- Gra2pesRegridConfig: regridding parameters and computed grid outputs

Author: Aaron G. Meyer
------------------------------------------------------------------------------
"""

import os
import yaml
import numpy as np


class Gra2pesConfig:
    """
    Main configuration class for GRA2PES data download and organization.

    Loads editable user inputs from the YAML file and defines constants
    for sector naming, path structures, and emission file naming.
    """

    # Weekday groupings used to split data by activity patterns
    day_type_details = {
        'satdy': [5],  # Saturday
        'sundy': [6],  # Sunday
        'weekdy': [0, 1, 2, 3, 4]  # Monday–Friday
    }

    # Emission sectors included in GRA2PES
    sector_details = {
        'AG': {'description': 'Agriculture'},
        'AVIATION': {'description': 'Aviation'},
        'COMM': {'description': 'Commercial'},
        'COOKING': {'description': 'Cooking'},
        'EGU': {'description': 'Electricity Generation'},
        'FUG': {'description': 'Fugitive'},
        'INDF': {'description': 'Industrial fuel'},
        'INDP': {'description': 'Industrial processes'},
        'INTERNATIONAL': {'description': 'International'},
        'OFFROAD': {'description': 'Off-road vehicles'},
        'OG': {'description': 'Oil and gas'},
        'ONROAD_DSL': {'description': 'On-road diesel'},
        'ONROAD_GAS': {'description': 'On-road gasoline'},
        'RAIL': {'description': 'Rail'},
        'RES': {'description': 'Residential'},
        'SHIPPING': {'description': 'Shipping'},
        'VCP': {'description': 'Volatile chemical products'},
        'WASTE': {'description': 'Waste'},
        'total': {'description': 'Total emissions across sectors'}
    }

    # Path and filename templates used in the workflow
    base_path_structure = '{parent_path}/{base_id}'
    base_fname_structure = '{year_str}{month_str}/{day_type}/GRA2PESv1.0_{sector}_{year_str}{month_str}_{day_type}_{hour_start}to{hour_end}Z.nc'
    regridded_path_structure = '{parent_path}/regridded{regrid_id}'
    regridded_day_subpath_structure = '{year_str}/{month_str}/{day_type}'
    regridded_fname_structure = '{sector}_regridded.nc'

    def __init__(self, yaml_path=None):
        """
        Load configuration from the YAML file and initialize key attributes.
        """

        if yaml_path is None:
            yaml_path = os.path.join(os.path.dirname(__file__), 'gra2pes_config.yaml')


        with open(yaml_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Load general GRA2PES settings
        self.months = self.config['months']
        self.years = self.config['years']
        self.data_source = self.config['data_source']
        self.ftp_credentials_path = self.config['ftp_credentials_path']
        self.parent_path = self.config['parent_path']
        self.base_id = self.config['base_id']
        self.extra_id_details = self.config.get('extra_id_details', {})

        # Set computed values
        self.day_types = list(self.day_type_details.keys())
        self.sectors = list(self.sector_details.keys())
        self.base_path = self.base_path_structure.format(
            parent_path=self.parent_path, base_id=self.base_id
        )


class Gra2pesRegridConfig:
    """
    Configuration class for regridding GRA2PES emission data.

    Initialized with a Gra2pesConfig instance to share values like
    `parent_path`. Provides logic for computing grid layout and file paths.
    """

    def __init__(self, base_config: Gra2pesConfig):
        """
        Load regridding settings from the YAML file (under key 'regrid').
        """
        regrid = base_config.config['regrid']
        self.config = base_config

        # Direct config values from YAML
        self.lat_spacing = regrid['lat_spacing']
        self.lon_spacing = regrid['lon_spacing']
        self.lat_center_range = tuple(regrid['lat_center_range'])
        self.lon_center_range = tuple(regrid['lon_center_range'])
        self.method = regrid['method']
        self.input_dims = tuple(regrid['input_dims'])
        self.weights_file = regrid['weights_file']
        self.encoding_details = regrid.get('encoding_details', {})

        # Derived config values
        if 'regrid_id_tag' in regrid.keys():
            self.regrid_id = f"{self.lat_spacing}x{self.lon_spacing}_{regrid['regrid_id_tag']}"
        else:
            self.regrid_id = f"{self.lat_spacing}x{self.lon_spacing}"
        self.regridded_path = self.get_regridded_path()
        self.grid_out = self.get_grid_out()

    def get_regridded_path(self):
        """
        Return the full path where regridded files should be saved.
        """
        return self.config.regridded_path_structure.format(
            parent_path=self.config.parent_path,
            regrid_id=self.regrid_id
        )

    def get_grid_out(self):
        """
        Construct the grid_out dictionary used in xESMF regridding.
        Includes both center and boundary coordinates.
        """
        return {
            'lat': np.arange(self.lat_center_range[0], self.lat_center_range[1], self.lat_spacing),
            'lon': np.arange(self.lon_center_range[0], self.lon_center_range[1], self.lon_spacing),
            'lat_b': np.arange(
                self.lat_center_range[0] - self.lat_spacing / 2,
                self.lat_center_range[1] + self.lat_spacing / 2,
                self.lat_spacing
            ),
            'lon_b': np.arange(
                self.lon_center_range[0] - self.lon_spacing / 2,
                self.lon_center_range[1] + self.lon_spacing / 2,
                self.lon_spacing
            )
        }
