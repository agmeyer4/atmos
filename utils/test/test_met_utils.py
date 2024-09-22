import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
from met_utils import VaisalaTPH

def main():
    # Your main code goes here
    vtph = VaisalaTPH()
    df = vtph.load_raw_file('/uufs/chpc.utah.edu/common/home/lin-group9/agm/EM27/ua/inst_data/met/raw_collected/vaisala_tph/20230804_tph.txt')
    print(df.head())
if __name__ == "__main__":
    main()