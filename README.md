# atmos
The atmos package is a python module for working with atmospheric data. It includes utilities designed to help with managing datetimes, pandas dataframes, geospatial data with xarray, emissions inventories, meteorological data, statistical regressions, and plotting routines. 

# Project Setup Instructions

1. Clone the repository to your local machine. This can be done with the following command:
```
> git clone https://github.com/agmeyer4/atmos.git
```

2. Create a conda environment from the yml file, then activate it. Ensure you are in the base git directory `atmos`. I much prefer using mamba to create it, conda can get stuck. (https://mamba.readthedocs.io/en/latest/)
```
> mamba env create -f environment.yml
```  
or 
```
> conda env create -f environment.yml
```
then
```
> conda activate atmos
```

# Structure
The atmos package is split into several subfolders, each with its own functionality. Below is a brief overview of each submodule and its purpose:
- **atmos.configs**: Contains configuration files for the atmos package, including default settings and parameters used throughout the package.
- **atmos.utils**: Contains utility functions for working with datetimes, pandas dataframes, xarray, meteorological data, and plotting routines.
- **atmos.inventories**: Provides functions for working with emissions inventories including downloading, regridding, and plotting. Each inventory has its own submodule, such as `atmos.inventories.gra2pes`.
- **atmos.ipynbs**: Contains Jupyter notebooks that demonstrate how to use the atmos package. These notebooks can be run in a Jupyter environment to explore the functionality of the package.
- **atmos.slurm** : Contains utilities for running commonly used scripts on a SLURM cluster. 

# Demos
The best way to get started with the atmos package is to run the Jupyter notebooks in the `atmos.ipynbs` directory. These notebooks provide examples of how to use the various functions and utilities provided by the package.

# Contact and Acknowledgements
This work is being carried out under the direction of Dr. John C. Lin in the Land-Atmosphere Interactions Research (LAIR) group at the University of Utah, Atmospheric Sciences Department. 

Please contact me directly with any questions or comments at agmeyer4@gmail.com. 

Produced by Aaron G. Meyer, 2025
