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

# Package Structure

The `atmos` package is organized into modular subdirectories, each serving a distinct purpose within the project:

- **`configs/`**  
  Contains configuration files in both `.py` and `.yaml` formats. These define default parameters, file paths, and settings for workflows and utilities across the package.

- **`utils/`**  
  A collection of general-purpose utilities for working with datetimes, pandas DataFrames, xarray objects, meteorological data, and plotting. These modules are designed to be reusable across workflows.

- **`workflows/`**  
  Houses structured, modular workflows for processing data. Each subfolder corresponds to a specific domain or inventory (e.g., `gra2pes`) and may include scripts for downloading, processing, and regridding data. More detailed readme files are provided in each workflow subdirectory to explain their specific purpose and usage.

- **`ipynbs/`**  
  Contains Jupyter notebooks that demonstrate how to use various parts of the package. These are intended for interactive exploration and example use cases.

- **`slurm/`**  
  Includes SLURM job submission scripts and templates for running workflows on HPC clusters.

# Demos
The best way to get started with the atmos package is to run the Jupyter notebooks in the `atmos.ipynbs` directory. These notebooks provide examples of how to use the various functions and utilities provided by the package.

# Contact and Acknowledgements
This work is being carried out under the direction of Dr. John C. Lin in the Land-Atmosphere Interactions Research (LAIR) group at the University of Utah, Atmospheric Sciences Department. 

Please contact me directly with any questions or comments at agmeyer4@gmail.com. 

Produced by Aaron G. Meyer, 2025
