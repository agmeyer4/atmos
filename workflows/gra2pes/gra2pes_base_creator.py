'''
Module: gra2pes_base_creator.py
Author: Aaron G. Meyer (agmeyer4@gmail.com)
Description:

This module contains classes and functions to download, extract, and organize "base" (the original files) GRA2PES data from the NOAA FTP server.

To use this module, edit the information in the gra2pes_config.py file to set the base path and other parameters. 

If using the "ftp" option, you will need to set up a credentials file with the structure:
username: <your_username>
password: <your_password>
The credentials file should be located in the path specified in the gra2pes_config.py file.

Next edit the main function as necessary to run either the "test" or "full" download. The test download will only download a small amount of data for testing, 
while the full download will download all data. You can also specify the years, months, and sectors you want to download.

Finally, run the script to download and extract the data by running < python gra2pes_base_creator.py > 
in the command line from this directory.

'''

##################################################################################################################################
#Import Packages
import os
import subprocess
import shutil
import filecmp
import time
import sys
import tarfile
from configs.gra2pes import gra2pes_config
from utils import gen_utils

##################################################################################################################################
# Define Classes

class Gra2pesDownload():
    """Class to download and extract GRA2PES data for a specific sector, year, and month
    
    Attributes:
    config (gra2pes_config.Gra2pesConfig) : configuration object for GRA2PES
    base_path (str) : base path where the data will be stored (part of the config)
    download_path (str) : path where the data will be downloaded (a temp folder called .download)
    base_download_url (str) : base url where the data lives online
    tar_filename_template (str) : template for the tar file name as stored in base_download_url
    """

    tar_filename_template = 'GRA2PES{version}_{sector}_{yearmonth}.tar.gz' #template for the tar file name as stored in base_download_url

    def __init__(self, config, data_source = 'https',credentials_path = None, min_space = '3Tb'):
        self.config = config #configuration object for GRA2PES
        self.base_path = config.base_path #base path where the data will be stored
        self.download_path = os.path.join(self.base_path, '.download') #path where the data will be downloaded (a temp folder called .download)
        os.makedirs(self.download_path, exist_ok=True) #create the download path if it doesn't exist
        self.data_source = data_source
        self.min_space = min_space
        if data_source == 'https':
            self.base_download_url = 'https://csl.noaa.gov/groups/csl4/gra2pes/datasets/data_{version}//{year}'#'https://data.nist.gov/od/ds/mds2-3520/'
        elif data_source == 'ftp':
            self.base_download_url = 'ftp://ftpshare.al.noaa.gov/GRA2PES{version}/{year}'
            if credentials_path is None:
                raise ValueError("credentials_path must be provided for ftp data source")
            self.credentials = gen_utils.read_credentials(credentials_path)
        else:
            raise ValueError(f"Data source {data_source} not recognized")

    def download_extract(self,sector,year,month):
        '''Main function to download GRA2PES data for a specific sector, year, and month and format the directories nicely
        
        Args:
        sector (str) : sector to download, from the self.config.sectors list
        year (int) : integer year to download
        month (int) : integer month to download
        '''
        if sector not in self.config.sectors:
            raise ValueError(f"sector {sector} not found in config")
        if year not in self.config.years:
            raise ValueError(f"year {year} not found in config")
        if month not in self.config.months:
            raise ValueError(f"month {month} not found in config")
        gen_utils.check_space(self.base_path,excep_thresh=self.min_space) #check if there is enough space in the base path
        
        tar_fname = self.get_tar_filename(sector, year, month)
        tar_full_url = self.get_tar_url(sector, year, month)
        self.download_tar(tar_full_url)
        self.extract_tar(tar_fname,extract_path=self.base_path)
        self.delete_tar(tar_fname)

    def get_tar_filename(self, sector, year, month):
        """Get the tar filename on the server for a specific sector, year, and month
        
        Args:
        sector (str) : sector to download, from the self.config.sectors list
        year (int) : integer year to download
        month (int) : integer month to download
        
        Returns:
        str : tar filename on the server
        """

        yearmonth = f"{year:04d}{month:02d}"
        return self.tar_filename_template.format(version=self.config.version, sector=sector, yearmonth=yearmonth)
    
    def get_tar_url(self, sector, year, month):
        """Get the full url to download the tar file for a specific sector, year, and month

        Args:
        sector (str) : sector to download, from the self.config.sectors list
        year (int) : integer year to download
        month (int) : integer month to download

        Returns:
        str : full url to download the tar file
        """

        filename = self.get_tar_filename(sector, year, month)
        pre_file_url = self.base_download_url.format(version=self.config.version,year=str(year)) #base url where the data lives online
        return os.path.join(pre_file_url,filename)
    
    def download_tar(self, tar_full_url):
        """Download the tar file from the full url to the download path
        
        Args:
        tar_full_url (str) : full url to download the tar file
        """

        print(f"Downloading {tar_full_url} to {self.download_path}")
        if self.data_source == 'https':
            command = ['wget',tar_full_url,'-P',self.download_path] #create the wget command 
        elif self.data_source == 'ftp':
            command = ['wget',f'--ftp-user={self.credentials["username"]}',f'--ftp-password={self.credentials["password"]}',
                       tar_full_url,'-P',self.download_path] #create the wget command

        proc = subprocess.Popen(command,stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) #call the command
        proc.communicate() #show the stdout and sterr 

    def extract_tar(self, tar_fname, extract_path):
        """Extract the tar file in the download path to the extract path
        
        Args:
        tar_fname (str) : tar filename to extract from the download path
        extract_path (str) : path to extract the tar file to
        """

        tar_path = os.path.join(self.download_path, tar_fname)
        print(f"Extracting {tar_path} to {extract_path}")
        fname = tar_fname
        command = ['tar','xvf',os.path.join(self.download_path,fname),'-C',extract_path] #define the tar extract command
        proc = subprocess.Popen(command,stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) #call the command
        proc.communicate() #output

    def delete_tar(self, tar_fname):
        """Delete the tar file in the download path 
        
        Args:
        tar_fname (str) : tar filename to delete from the download path
        """

        print(f"Deleting {tar_fname}")
        os.remove(os.path.join(self.download_path,tar_fname))

class Gra2pesDownloadExtra():
    """Class to download and extract GRA2PES extra data provided by Colin
    
    Attributes:
    config (gra2pes_config.Gra2pesConfig) : configuration object for GRA2PES
    base_path (str) : base path where the data will be stored (part of config)
    extra_id (str) : extra id for the data (e.g. 'methane')
    download_path (str) : path where the data will be downloaded (a temp folder called .download)
    base_download_url (str) : base url where the data lives online
    tar_filename_template (str) : template for the tar file name as stored in base_download_url
    """

    def __init__(self, config, extra_id, data_source = 'https', credentials_path = None):
        self.config = config #configuration object for GRA2PES
        self.base_path = config.base_path #base path where the data will be stored
        self.extra_id = extra_id
        if extra_id not in config.extra_id_details.keys():
            raise ValueError(f"extra_id {extra_id} not found in config.extra_id_details")
        self.extra_id_details = config.extra_id_details[extra_id] #extra id details from the config
        self.tar_filename_template = 'GRA2PES_{sector}_{year}_{extra_id}.tar.gz' #template for the tar file name as stored in base_download_url
        self.download_path = os.path.join(self.base_path, f'.download/{extra_id}') #path where the data will be downloaded (a temp folder called .download)
        os.makedirs(self.download_path, exist_ok=True) #create the download path if it doesn't exist
        self.data_source = data_source
        if data_source == 'https':
            self.base_download_url = 'https://csl.noaa.gov/groups/csl4/gra2pes/datasets/data_{version}//{year}/{extra_id_folder_detail}'
        elif data_source == 'ftp':
            self.base_download_url = 'ftp://ftpshare.al.noaa.gov/GRA2PES{version}/{year}/{extra_id_folder_detail}'
        if credentials_path is None:
            raise ValueError("credentials_path must be provided for ftp data source")
        self.credentials = gen_utils.read_credentials(credentials_path)

    def download_and_extract(self,sector,year,mvpath = None):
        '''Main function to download extra GRA2PES data for a specific sector, year, and month and format the directories nicely
        
        Args:
        sector (str) : sector to download, from the self.config.sectors list
        year (int) : integer year to download
        '''
        if sector not in self.config.sectors:
            raise ValueError(f"sector {sector} not found in config")
        if year not in self.config.years:
            raise ValueError(f"year {year} not found in config")
        if year not in self.extra_id_details['years']:
            raise ValueError(f"year {year} not found in config.extra_id_details['years'] for extra_id {self.extra_id}")
        
        gen_utils.check_space(self.base_path,excep_thresh='1Tb') #check if there is enough space in the base path
        tar_fname = self.get_tar_filename(sector, year)
        tar_full_url = self.get_tar_url(sector, year)
        if mvpath is None:
            self.download_tar(tar_full_url)
        else:
            old_loc = os.path.join(mvpath,tar_fname)
            shutil.copy(old_loc, os.path.join(self.download_path, tar_fname))
        self.extract_tar(tar_fname)
        self.delete_tar(tar_fname)


    def get_tar_filename(self, sector, year):
        """Get the tar filename on the server for a specific sector, year, and month
        
        Args:
        sector (str) : sector to download, from the self.config.sectors list
        year (int) : integer year to download
        
        Returns:
        str : tar filename on the server
        """

        year = f"{year:04d}"
        return self.tar_filename_template.format(version = self.config.version, sector=sector, year=year, extra_id = self.extra_id)
    
    def get_tar_url(self, sector, year):
        """Get the full url to download the tar file for a specific sector, year, and month

        Args:
        sector (str) : sector to download, from the self.config.sectors list
        year (int) : integer year to download
        month (int) : integer month to download

        Returns:
        str : full url to download the tar file
        """

        filename = self.get_tar_filename(sector, year)
        pre_file_url = self.base_download_url.format(version= self.config.version, year=str(year), extra_id_folder_detail=self.extra_id_details['folder_name']) 
        return os.path.join(pre_file_url,filename)
    
    def download_tar(self, tar_full_url):
        """Download the tar file from the full url to the download path
        
        Args:
        tar_full_url (str) : full url to download the tar file
        """

        print(f"Downloading {tar_full_url} to {self.download_path}")

        if self.data_source == 'https':
            command = ['wget',tar_full_url,'-P',self.download_path]
        elif self.data_source == 'ftp':
            command = ['wget',f'--ftp-user={self.credentials["username"]}',f'--ftp-password={self.credentials["password"]}',
                        tar_full_url,'-P',self.download_path] #create the wget command

        proc = subprocess.Popen(command,stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) #call the command
        proc.communicate() #show the stdout and sterr 

    def extract_tar(self, tar_fname):
        """Extract the tar file in the download path 
        
        Args:
        tar_fname (str) : tar filename to extract from the download path
        """

        print(f"Extracting {tar_fname}")
        fname = tar_fname
        command = ['tar','xvf',os.path.join(self.download_path,fname),'-C',self.download_path] #define the tar extract command
        proc = subprocess.Popen(command,stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) #call the command
        proc.communicate() #output

    def delete_tar(self, tar_fname):
        """Delete the tar file in the download path 
        
        Args:
        tar_fname (str) : tar filename to delete from the download path
        """

        print(f"Deleting {tar_fname}")
        os.remove(os.path.join(self.download_path,tar_fname))


class OrganizeExtraDownload():
    """Class to organize the extra data downloaded by Gra2pesDownloadExtra

    Attributes:
    base_path (str) : base path where the data will be stored
    download_path (str) : path where the data will be downloaded (a temp folder called .download)
    extra_id_details (str) : extra id for the data (e.g. 'methane')
    """

    def __init__(self, base_path, extra_id):
        self.base_path = base_path
        self.download_path = os.path.join(self.base_path, '.download')
        self.extra_id = extra_id

    def organize_extra(self):
        """Main function to organize the extra data downloaded by Gra2pesDownloadExtra
        
        Moves data from the .download/extra_id folder to the base_path/extra_id folder, making sure the structure matches
        the main data in the base_path. Then deletes the .download/extra_id folder."""

        print(f'Organizing extra "{self.extra_id}"')
        extra_download_path = os.path.join(self.download_path,self.extra_id) #path to the extra download
        extra_base_path = self.create_extra_in_base(self.extra_id) #path to the extra_id folder in the base_path
        extra_download_matchpath = self.find_multifolder_path(extra_download_path)  # find where in the extra download path there are multiple folders (means where ate the date folder)
        
        self.move_at_multifolder(extra_download_matchpath,extra_base_path,dirs_exist_ok=True) #move the data from the extra download path to the extra base path
        self.delete_extra_download(extra_download_path) #delete the extra download path

    def delete_extra_download(self,extra_download_path):
        """Delete the extra download path after moving the data

        Args:
        extra_download_path (str) : path to the extra download folder
        """

        shutil.rmtree(extra_download_path)

    def move_at_multifolder(self,src,dst,dirs_exist_ok=False):
        """Move the data from the src path to the dst path, making sure the structure matches the main data in the base_path

        Args:
        src (str) : source path to move data from
        dst (str) : destination path to move data to
        dirs_exist_ok (bool) : whether to allow the destination directory to exist, if False, will raise an error if the destination directory
        """

        shutil.copytree(src,dst,dirs_exist_ok=dirs_exist_ok)

    def find_multifolder_path(self,src):
        """Find where in the src path there are multiple folders (means where we start the date folder)

        Args:
        src (str) : source path to find the multiple folders in

        Returns:
        str : path to the parent dir where there are multiple folders
        """

        nitems = len(os.listdir(src)) #number of items in the src path
        if nitems == 1: #if there is only one item in the src path, go to the next folder
            newsrc = os.path.join(src,os.listdir(src)[0]) #new src path
            return self.find_multifolder_path(newsrc) #recursive call
        else: #if there are multiple items in the src path, return the parent dir
            return src

    def create_extra_in_base(self,extra_id):
        """Create the extra_id folder in the base_path

        Args:
        extra_id (str) : extra id for the data (e.g. 'methane')

        Returns:
        str : path to the extra_id folder in the base_path
        """

        extra_path = os.path.join(self.base_path,extra_id)
        os.makedirs(extra_path, exist_ok=True)
        return extra_path

def compare_base_and_extra(base_path,extra_id):
    """Compare the base and extra directories to make sure they are the same structure

    Args:
    base_path (str) : base path where the data is stored
    extra_id (str) : extra id for the data (e.g. 'methane')

    Returns:
    bool : whether the base and extra directories are the same structure
    """

    main_path = base_path #main path is the base path
    extra_path = os.path.join(base_path,extra_id) #extra path is the base path/extra_id

    main_subdirs = sorted([d for d in gen_utils.listdir_visible(main_path) if d != extra_id]) #get the directories in the main path, excluding the extra_id
    extra_subdirs = sorted(gen_utils.listdir_visible(extra_path)) #get the directories in the extra path

    if main_subdirs != extra_subdirs: #if the main and extra directories do not match, raise an error
        raise ValueError(f"Main and extra directories do not match: \nmain - {main_path}   {main_subdirs}\nextra - {extra_path}  {extra_subdirs}")

    for yearmonth in main_subdirs: #for each yearmonth in the main subdirs
        main_dir = os.path.join(main_path,yearmonth) #main directory is the main path/yearmonth
        extra_dir = os.path.join(extra_path,yearmonth) #extra directory is the extra path/yearmonth
        try: #try to compare the directories exactly
            compare_dirs_exact(main_dir,extra_dir)
        except ValueError as e: #if there is an error, print the error and return False
            print(e)
            return False
    return True

def compare_dirs_exact(dir1, dir2):
    """Compare two directories exactly, raising an error if they are not the same

    Args:
    dir1 (str) : first directory to compare
    dir2 (str) : second directory to compare

    Raises:
    ValueError : if the directories are not the same
    """

    differences = [] #list to store differences

    def _compare_dirs(dir1, dir2): #recursive function to compare directories
        dir_cmp = filecmp.dircmp(dir1, dir2) #compare the directories
        if dir_cmp.left_only or dir_cmp.right_only: #if there are directories that are only in one directory, add them to the differences list
            differences.append(f"Directories are not the same:\n{dir1}: {dir_cmp.left_only}\n{dir2}: {dir_cmp.right_only}") 

        for common_dir in dir_cmp.common_dirs: #for each common directory, recursively call the function
            _compare_dirs(os.path.join(dir1, common_dir), os.path.join(dir2, common_dir)) 

    _compare_dirs(dir1, dir2) #call the recursive function

    if differences: #if there are differences, raise an error
        raise ValueError("\n".join(differences))

def main():
    """Main function to download and extract GRA2PES data for a specific sector, year, and month and format the directories nicely""" 
     
    t1 = time.time() # start time
    print(f'Beginning GRA2PES base data download and organization at {t1}')
    config = gra2pes_config.Gra2pesConfig() #load the config file

    credentials_path = config.ftp_credentials_path #path to the credentials file
    data_source = config.data_source #data source (ftp or https)
    main_downloader = Gra2pesDownload(config, data_source=data_source,credentials_path=credentials_path) 

    # #### Test Download ####
    # years = config.years
    # months = [1] #config.months
    # sectors = ['AG','AVIATION']#config.sectors
    # print(f'Downloading and extracting for years: {years}, months: {months}, sectors: {sectors}')
    # for year in years:
    #     for month in months:
    #         extract_path = os.path.join(config.base_path,f'{year:04d}{month:02d}')
    #         if os.path.exists(extract_path):
    #             print(f'Skipping {year}-{month}, already exists at {extract_path}')
    #             continue
    #         for sector in sectors:
    #             print(f'\nDownloading and extracting {sector} for {year}-{month}')
    #             main_downloader.download_extract(sector,year,month)

    # #### Extra Download ####
    #Below downloads the extra methane data. 
    # extra_id = 'methane'
    # for year in years:
    #     for sector in sectors:
    #         print(f'\nDownloading and extracting {sector} for {year} {extra_id}')
    #         extra_downloader = Gra2pesDownloadExtra(config, extra_id,data_source=data_source, credentials_path=credentials_path)
    #         extra_downloader.download_and_extract(sector,year)#,mvpath=tar_loc)
    #         extra_organizer = OrganizeExtraDownload(config.base_path,extra_id)
    #         extra_organizer.organize_extra()



    #### Full Download ####
    years = config.years
    months = config.months
    sectors = config.sectors
    print(f'Downloading and extracting GRA2PES {config.version} for years: {years}, months: {months}, sectors: {sectors}')
    for year in years:
        for month in months:
            extract_path = os.path.join(config.base_path,f'{year:04d}{month:02d}')
            if os.path.exists(extract_path):
                print(f'Skipping {year}-{month}, already exists at {extract_path}')
                continue
            for sector in sectors:
                print(f'\nDownloading and extracting {sector} for {year}-{month}')
                main_downloader.download_extract(sector,year,month)

    #### Extra Download ####
    # Below downloads the extra methane data. 
    # extra_id = 'methane'
    # for year in years:
    #     for sector in sectors:
    #         print(f'\nDownloading and extracting {sector} for {year} {extra_id}')
    #         extra_downloader = Gra2pesDownloadExtra(config, extra_id, credentials_path=credentials_path)
    #         extra_downloader.download_and_extract(sector,year)#,mvpath=tar_loc)
    #         extra_organizer = OrganizeExtraDownload(config.base_path,extra_id)
    #         extra_organizer.organize_extra()
    # try:
    #     good_compare = compare_base_and_extra(config.base_path,extra_id) # compare the base and extra directories to make sure they are the same structure
    # except ValueError as e:
    #     print(f"Error comparing base and extra directories: {e}")

    t2 = time.time()
    print(f"Time taken: {round(t2-t1)} seconds")

if __name__ == '__main__':
    main()