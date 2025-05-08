#Import Packages
import os
import subprocess
import shutil
import glob
import filecmp
import numpy as np
import pandas as pd
import git

##################################################################################################################################
# Define Functions
def get_githash():
    '''Gets the git hash of the current code to save as metadata in regridded/changed objects
    
    Returns:
    githash (str) : the hash of the current git commit
    '''
    repo = git.Repo(search_parent_directories=True)
    sha = repo.head.object.hexsha
    return sha

def check_space(path,excep_thresh='8Tb'):
    '''Checks the amount of space on a filesystem given a path, and raise an error if the amount of space is below the threshold
    
    Args:
    path (str) : path to check how much room there would be based on the filesystem
    excep_thresh (str) : threshold below which to raise an exception, defaults '8T'
    
    Returns:
    usage_details (tuple) : output of shutil.disk_usage(path)
    
    Raises:
    MemoryError : if the amount of space on the filesystem containing the path is below the excep_thresh
    '''

    disk_usage = shutil.disk_usage(path)
    total_bytes,used_bytes,free_bytes = int(disk_usage.total), int(disk_usage.used), int(disk_usage.free)
    thresh_bytes = human_to_bytes(excep_thresh)
    if free_bytes < thresh_bytes:
        print(f'Path: {path}')
        print(f'Total space: {bytes_to_human(total_bytes)}')
        print(f'Used space: {bytes_to_human(used_bytes)}')
        print(f'Free space: {bytes_to_human(free_bytes)}')
        print(f'Required space (a user defined property): {excep_thresh}\n')
        raise MemoryError(f'There is less than {excep_thresh} in the destination filesystem. Ensure there is enough room for your data download, or change the threshold')
    else:
        print(f'Found {bytes_to_human(free_bytes)} free space')
        return disk_usage
    
def human_to_bytes(human_readable, units=['b', 'Kb', 'Mb', 'Gb', 'Tb', 'Pb']):
    '''Makes a human readable storage size into a bytes integer
    #TODO Different definitions for this and the other direction (1000 bytes per kb vs 1024)
    
    Args:
    human_readable (str) : a human readable string representing space (something like 2.1Gb, or 5Tb)
    units (list) : list of strings defining the units 
    
    Returns:
    bytes (int) : integer representing how many bytes the human readable string is
    '''

    # Split the input string into value and unit
    value_str = ''.join([ch for ch in human_readable if ch.isdigit() or ch == '.'])
    unit_str = ''.join([ch for ch in human_readable if not ch.isdigit() and ch != '.']).strip()
    value = float(value_str)# Convert value string to a float
    
    # Find the unit in the provided units list
    try:
        unit_index = units.index(unit_str)
    except ValueError:
        raise ValueError(f"Invalid unit '{unit_str}' in input string.")
    
    multiplier = 1024 ** unit_index # Calculate the multiplier based on the unit index
    bytes = int(value * multiplier)   # Convert the human-readable value to bytes    
    return bytes

def bytes_to_human(bytes,units=['b','Kb','Mb','Gb','Tb','Pb']):
    '''Converts an integer bytes into a human readable string notation
    #TODO Different definitions for this and the other direction (1000 bytes per kb vs 1024)
    
    Args: 
    bytes (int) : integer bytes
    units (list) : unit convention for factors of 10 
    
    Returns (str) : the human readable string
    '''

    return str(bytes) + units[0] if bytes < 1024 else bytes_to_human(bytes>>10, units[1:])

def listdir_visible(path,add_path = False):
    '''Function to list only "visible" files/folders (not starting with a period)
    
    Args:
    path (str) : the filepath to list elements of
    
    Returns:
    vis_list (list) : list of files within the input path not starting with a period
    '''
    
    vis_list = [f for f in os.listdir(path) if not f.startswith('.')]
    if add_path:
        vis_list = [os.path.join(path,f) for f in vis_list]
    return vis_list

def read_credentials(fullpath):
    """Read the credentials file and return the username and password
    
    Args:
    fullpath (str) : full path to the credentials file
    
    Returns:
    dict : dictionary with keys 'username' and 'password'
    """
    credentials ={}
    with open(fullpath) as f:
        lines = f.readlines()
        for line in lines:
            key,value = line.strip().split('=')
            credentials[key] = value
    return credentials

def calculate_exponent(value):
    """Calculate the exponent based on the magnitude of the value."""
    return int(np.floor(np.log10(abs(value))))  # Get the exponent of the largest magnitude


def main():
    dir1 = '/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES/base_v1.0/202102'
    dir2 = '/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES/base_v1.0/methane/202102'
if __name__ == '__main__':
    main()