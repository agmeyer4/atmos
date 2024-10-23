'''
Module: gra2pes_download.py
Author: Aaron G. Meyer (agmeyer4@gmail.com)
Description:

'''

##################################################################################################################################
#Import Packages
import os
import subprocess
import shutil
import glob
import filecmp
import sys
import time
import gra2pes_config, gra2pes_utils
sys.path.append(os.path.join(os.path.dirname(__file__),'../..'))
from utils import gen_utils

##################################################################################################################################
# Define Classes

class Gra2pesDownload():
    """Class to download and extract GRA2PES data for a specific sector, year, and month
    
    Attributes:
    config (gra2pes_config.Gra2pesConfig) : configuration object for GRA2PES
    base_path (str) : base path where the data will be stored
    download_path (str) : path where the data will be downloaded (a temp folder called .download)
    base_download_url (str) : base url where the data lives online
    tar_filename_template (str) : template for the tar file name as stored in base_download_url
    """

    tar_filename_template = 'GRA2PESv1.0_{sector}_{yearmonth}.tar.gz' #template for the tar file name as stored in base_download_url

    def __init__(self, config, base_path, data_source = 'https',credentials_path = None, min_space = '1Tb'):
        self.config = config #configuration object for GRA2PES
        self.base_path = base_path #base path where the data will be stored
        self.download_path = os.path.join(self.base_path, '.download') #path where the data will be downloaded (a temp folder called .download)
        os.makedirs(self.download_path, exist_ok=True) #create the download path if it doesn't exist
        self.data_source = data_source
        self.min_space = min_space
        if data_source == 'https':
            self.base_download_url = 'https://data.nist.gov/od/ds/mds2-3520/'
        elif data_source == 'ftp':
            self.base_download_url = 'ftp://ftp.al.noaa.gov'
            if credentials_path is None:
                raise ValueError("credentials_path must be provided for ftp data source")
            self.credentials = gen_utils.read_credentials(credentials_path)
        else:
            raise ValueError(f"Data source {data_source} not recognized")

    def download_extract(self,sector,year,month):
        '''Main function to download GRA2PES data for a specific sector, year, and month and format the directories nicely
        
        Args:
        sector (str) : sector to download, from the self.config.sector_details.keys() dictionary
        year (int) : integer year to download
        month (int) : integer month to download
        '''
        if sector not in self.config.sector_details.keys():
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
        sector (str) : sector to download, from the self.config.sector_details.keys() dictionary
        year (int) : integer year to download
        month (int) : integer month to download
        
        Returns:
        str : tar filename on the server
        """

        yearmonth = f"{year:04d}{month:02d}"
        return self.tar_filename_template.format(sector=sector, yearmonth=yearmonth)
    
    def get_tar_url(self, sector, year, month):
        """Get the full url to download the tar file for a specific sector, year, and month

        Args:
        sector (str) : sector to download, from the self.config.sector_details.keys() dictionary
        year (int) : integer year to download
        month (int) : integer month to download

        Returns:
        str : full url to download the tar file
        """

        filename = self.get_tar_filename(sector, year, month)
        return os.path.join(self.base_download_url,filename)
    
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

        print(f"Extracting {tar_fname}")
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
    base_path (str) : base path where the data will be stored
    download_path (str) : path where the data will be downloaded (a temp folder called .download)
    base_download_url (str) : base url where the data lives online
    tar_filename_template (str) : template for the tar file name as stored in base_download_url
    """


    def __init__(self, config, base_path, extra_id, credentials_path = None):
        self.config = config #configuration object for GRA2PES
        self.base_path = base_path #base path where the data will be stored
        self.extra_id = extra_id
        self.tar_filename_template = 'GRA2PES_{sector}_{year}_{extra_id}.tar.gz' #template for the tar file name as stored in base_download_url

        self.download_path = os.path.join(self.base_path, f'.download/{extra_id}') #path where the data will be downloaded (a temp folder called .download)
        os.makedirs(self.download_path, exist_ok=True) #create the download path if it doesn't exist
        self.base_download_url = f'ftp://ftp.al.noaa.gov/{extra_id}'
        if credentials_path is None:
            raise ValueError("credentials_path must be provided for ftp data source")
        self.credentials = gen_utils.read_credentials(credentials_path)

    def download_and_extract(self,sector,year):
        '''Main function to download GRA2PES data for a specific sector, year, and month and format the directories nicely
        
        Args:
        sector (str) : sector to download, from the self.config.sector_details.keys() dictionary
        year (int) : integer year to download
        '''
        if sector not in self.config.sector_details.keys():
            raise ValueError(f"sector {sector} not found in config")
        if year not in self.config.years:
            raise ValueError(f"year {year} not found in config")
        
        gen_utils.check_space(self.base_path,excep_thresh='1Tb') #check if there is enough space in the base path
        tar_fname = self.get_tar_filename(sector, year)
        tar_full_url = self.get_tar_url(sector, year)
        #self.download_tar(tar_full_url)
        self.extract_tar(tar_fname)
        self.delete_tar(tar_fname)


    def get_tar_filename(self, sector, year):
        """Get the tar filename on the server for a specific sector, year, and month
        
        Args:
        sector (str) : sector to download, from the self.config.sector_details.keys() dictionary
        year (int) : integer year to download
        
        Returns:
        str : tar filename on the server
        """

        year = f"{year:04d}"
        return self.tar_filename_template.format(sector=sector, year=year, extra_id = self.extra_id)
    
    def get_tar_url(self, sector, year):
        """Get the full url to download the tar file for a specific sector, year, and month

        Args:
        sector (str) : sector to download, from the self.config.sector_details.keys() dictionary
        year (int) : integer year to download
        month (int) : integer month to download

        Returns:
        str : full url to download the tar file
        """

        filename = self.get_tar_filename(sector, year)
        return os.path.join(self.base_download_url,filename)
    
    def download_tar(self, tar_full_url):
        """Download the tar file from the full url to the download path
        
        Args:
        tar_full_url (str) : full url to download the tar file
        """

        print(f"Downloading {tar_full_url} to {self.download_path}")

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

    def __init__(self, base_path, extra_id):
        self.base_path = base_path
        self.download_path = os.path.join(self.base_path, '.download')
        self.extra_id_details = extra_id

    def organize_extra(self):
        print(f'Organizing extra "{self.extra_id}"')
        extra_download_path = os.path.join(self.download_path,self.extra_id)
        extra_base_path = self.create_extra_in_base(self.extra_id)
        extra_download_matchpath = self.find_multifolder_path(extra_download_path)
        
        self.move_at_multifolder(extra_download_matchpath,extra_base_path,dirs_exist_ok=True)
        self.delete_download_at_multifolder(extra_download_matchpath)

    def delete_download_at_multifolder(self,extra_download_matchpath):
        shutil.rmtree(extra_download_matchpath)

    def move_at_multifolder(self,src,dst,dirs_exist_ok=False):
        shutil.copytree(src,dst,dirs_exist_ok=dirs_exist_ok)

    def find_multifolder_path(self,src):
        nitems = len(os.listdir(src))
        if nitems == 1:
            newsrc = os.path.join(src,os.listdir(src)[0])
            return self.find_multifolder_path(newsrc)
        else:
            return src

    def create_extra_in_base(self,extra_id):
        extra_path = os.path.join(self.base_path,extra_id)
        os.makedirs(extra_path, exist_ok=True)
        return extra_path

def compare_base_and_extra(base_path,extra_id):
    main_path = base_path
    extra_path = os.path.join(base_path,extra_id)

    main_subdirs = sorted([d for d in gen_utils.listdir_visible(main_path) if d != extra_id])
    extra_subdirs = sorted(gen_utils.listdir_visible(extra_path))

    if main_subdirs != extra_subdirs:
        raise ValueError(f"Main and extra directories do not match: \nmain - {main_path}   {main_subdirs}\nextra - {extra_path}  {extra_subdirs}")

    for yearmonth in main_subdirs:
        main_dir = os.path.join(main_path,yearmonth)
        extra_dir = os.path.join(extra_path,yearmonth)
        try:
            compare_dirs_exact(main_dir,extra_dir)
        except ValueError as e:
            print(e)
            return False
    return True

def compare_dirs_exact(dir1, dir2):
    differences = []

    def _compare_dirs(dir1, dir2):
        dir_cmp = filecmp.dircmp(dir1, dir2)
        if dir_cmp.left_only or dir_cmp.right_only:
            differences.append(f"Directories are not the same:\n{dir1}: {dir_cmp.left_only}\n{dir2}: {dir_cmp.right_only}")

        for common_dir in dir_cmp.common_dirs:
            _compare_dirs(os.path.join(dir1, common_dir), os.path.join(dir2, common_dir))

    _compare_dirs(dir1, dir2)

    if differences:
        raise ValueError("\n".join(differences))

def main():
    t1 = time.time()
    base_path = '/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES/base_v1.0'
    credentials_path = '/uufs/chpc.utah.edu/common/home/u0890904/credentials/ftp_gra2pes_credentials.txt'
    config = gra2pes_config.Gra2pesConfig()
    main_downloader = Gra2pesDownload(config, base_path, data_source='ftp',credentials_path=credentials_path)
    
    years = config.years
    months = config.months
    sectors = config.sector_details.keys()
    for year in years:
        for month in months:
            for sector in sectors:
                print(f'\nDownloading and extracting {sector} for {year}-{month}')
                main_downloader.download_extract(sector,year,month)

    # extra_id = 'methane'
    # for year in years:
    #     for sector in sectors:
    #         print(f'\nDownloading and extracting {sector} for {year} {extra_id}')
    #         extra_downloader = Gra2pesDownloadExtra(config, base_path, extra_id, credentials_path=credentials_path)
    #         extra_downloader.download_and_extract(sector,year)

    #         extra_organizer = OrganizeExtraDownload(base_path,extra_id)
    #         extra_organizer.organize_extra()

    # good_compare = compare_base_and_extra(base_path,extra_id)

    t2 = time.time()
    print(f"Time taken: {round(t2-t1)} seconds")

if __name__ == '__main__':
    main()