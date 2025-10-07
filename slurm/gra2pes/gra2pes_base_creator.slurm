#!/bin/bash
#SBATCH --nodes=1
#SBATCH --account=carbon-kp
#SBATCH --partition=carbon-kp
#SBATCH -o ./logs/gra2pes_base_creator_%A.out

source activate atmos
cd /uufs/chpc.utah.edu/common/home/u0890904/LAIR_1/atmos/
python -u -m workflows.gra2pes.gra2pes_base_creator