#!/bin/bash
#SBATCH --nodes=1
#SBATCH --account=carbon-kp
#SBATCH --partition=carbon-kp
#SBATCH -o ./logs/gra2pes_regrid_%A.out

source activate atmos1
cd /uufs/chpc.utah.edu/common/home/u0890904/LAIR_1/atmos
python -u -m workflows.gra2pes.gra2pes_regrid

# Extract the path from the 4th line of the log file
details_path=$(sed -n '4s/^Saving regridded data to //p' ./slurm/gra2pes/logs/gra2pes_regrid_${SLURM_JOB_ID}.out)/details

# Copy the log file to the path returned by the Python script
echo "Copying log file to $details_path"
cp ./slurm/gra2pes/logs/gra2pes_regrid_${SLURM_JOB_ID}.out "$details_path"


