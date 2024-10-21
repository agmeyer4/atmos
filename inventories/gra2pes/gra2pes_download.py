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
import time
import gra2pes_config, gra2pes_utils

##################################################################################################################################
# Define Classes

class Gra2pesDownload():
    """Class to download and format GRA2PES data for a specific sector, year, and month
    
    Attributes:
    config (gra2pes_config.Gra2pesConfig) : configuration object for GRA2PES
    base_path (str) : base path where the data will be stored
    download_path (str) : path where the data will be downloaded (a temp folder called .download)
    base_download_url (str) : base url where the data lives online
    tar_filename_template (str) : template for the tar file name as stored in base_download_url
    """

    tar_filename_template = 'GRA2PESv1.0_{sector}_{yearmonth}.tar.gz' #template for the tar file name as stored in base_download_url

    def __init__(self, config, base_path, data_source = 'https',credentials_path = None):
        self.config = config #configuration object for GRA2PES
        self.base_path = base_path #base path where the data will be stored
        self.download_path = os.path.join(self.base_path, '.download') #path where the data will be downloaded (a temp folder called .download)
        os.makedirs(self.download_path, exist_ok=True) #create the download path if it doesn't exist
        self.data_source = data_source
        if data_source == 'https':
            self.base_download_url = 'https://data.nist.gov/od/ds/mds2-3520/'
        elif data_source == 'ftp':
            self.base_download_url = 'ftp://ftp.al.noaa.gov'
            if credentials_path is None:
                raise ValueError("credentials_path must be provided for ftp data source")
            self.credentials = gra2pes_utils.read_credentials(credentials_path)
        else:
            raise ValueError(f"Data source {data_source} not recognized")

    def download_and_format(self,sector,year,month):
        '''Main function to download GRA2PES data for a specific sector, year, and month and format the directories nicely
        
        Args:
        sector (str) : sector to download, from the self.config.sector_details.keys() dictionary
        year (int) : integer year to download
        month (int) : integer month to download
        '''
        print(f"Downloading {sector} data for {year}-{month}")
        if sector not in self.config.sector_details.keys():
            raise ValueError(f"sector {sector} not found in config")
        if year not in self.config.sector_details[sector]['years']:
            raise ValueError(f"year {year} not found in config")
        if month not in self.config.month_list:
            raise ValueError(f"month {month} not found in config")
        
        tar_fname = self.get_tar_filename(sector, year, month)
        tar_full_url = self.get_tar_url(sector, year, month)
        print(tar_full_url)
        self.download_tar(tar_full_url)
        self.extract_tar(tar_fname)


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
            
        #print(' '.join(command))

        proc = subprocess.Popen(command,stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) #call the command
        proc.communicate() #show the stdout and sterr 

    def extract_tar(self, tar_fname):
        """Extract the tar file in the download path 
        
        Args:
        tar_fname (str) : tar filename to extract from the download path
        """

        print(f"Extracting {tar_fname}")
        path = self.download_path
        fname = tar_fname
        command = ['tar','xvf',os.path.join(path,fname),'-C',path] #define the tar extract command
        proc = subprocess.Popen(command,stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT) #call the command
        proc.communicate() #output

def main():
    t1 = time.time()
    base_path = '/uufs/chpc.utah.edu/common/home/lin-group9/agm/inventories/GRA2PES/base_v1.0'
    credentials_path = '/uufs/chpc.utah.edu/common/home/u0890904/credentials/ftp_gra2pes_credentials.txt'
    config = gra2pes_config.Gra2pesConfig()
    downloader = Gra2pesDownload(config, base_path, data_source='ftp',credentials_path=credentials_path)
    
    downloader.download_and_format('AG',2021,1)
    #print(downloader.__dict__)

    # for sector in config.sector_details.keys():
    #     downloader.download_and_format(sector, 2021, 1)
    #     time.sleep(1)

    t2 = time.time()
    print(f"Time taken: {round(t2-t1)} seconds")

if __name__ == '__main__':
    main()