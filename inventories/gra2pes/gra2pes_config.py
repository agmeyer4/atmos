class Gra2pesConfig():
    base_download_url = 'https://data.nist.gov/od/ds/mds2-3520/'
    tar_filename_template = 'GRA2PESv1.0_{sector}_{yearmonth}.tar.gz'

    sector_details = {
        'AG'
    }

    def __init__(self):
        pass

    def get_filename(self, sector, year, month):
        yearmonth = f"{year:04d}{month:02d}"
        return self.tar_filename_template.format(sector=sector, yearmonth=yearmonth)
    
    def get_url(self, sector, year, month):
        filename = self.get_filename(sector, year, month)
        return f"{self.base_download_url}{filename}"