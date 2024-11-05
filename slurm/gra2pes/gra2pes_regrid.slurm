#!/bin/bash
#SBATCH --nodes=1
#SBATCH --account=carbon-kp
#SBATCH --partition=carbon-kp
#SBATCH -o ./logs/gra2pes_regrid_%A.out

source activate atmos_column1
cd /uufs/chpc.utah.edu/common/home/u0890904/LAIR_1/atmos/inventories/gra2pes
python -u gra2pes_regrid.py

# Extract the path from the 4th line of the log file
details_path=$(sed -n '4s/^Saving regridded .nc files to //p' ./slurm/logs/csl_regrid_${SLURM_JOB_ID}.out)/details

# Copy the log file to the path returned by the Python script
echo "Copying log file to $details_path"
cp ./slurm/logs/csl_regrid_${SLURM_JOB_ID}.out "$details_path"
