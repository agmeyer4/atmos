#!/bin/bash
#SBATCH --nodes=1
#SBATCH --account=carbon-kp
#SBATCH --partition=carbon-kp
#SBATCH -o ./logs/gra2pes_slice_retime_%A.out

source activate atmos1
cd /uufs/chpc.utah.edu/common/home/u0890904/LAIR_1/atmos/
python -u -m workflows.gra2pes.gra2pes_slice_retime