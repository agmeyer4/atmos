"""Configuration for met tools"""


class MetConfig:
    def __init__(self):
        self.default_vars = {
            'et': {'description': 'Epoch time -- seconds since 1970-01-01 00:00:00 UTC', 'units': 's', 'dtype':'float'},
            'dt': {'description': 'Date time', 'units': 'na', 'dtype':'datetime.datetime'},
            'pres': {'description': 'Atmospheric Pressure','units': 'hPa', 'dtype':'float'},
            'temp': {'description': 'Temperature','units': 'C', 'dtype':'float'},
            'rh': {'description': 'Relative Humidity','units': '%', 'dtype':'float'},
            'ws': {'description': 'Wind Speed','units': 'm/s', 'dtype':'float'},
            'wd': {'description': 'Wind Direction (clockwise from N)','units': 'degrees', 'dtype':'float'},
            'u': {'description': 'u component of wind (>0 = from west)','units': 'm/s', 'dtype':'float'},
            'v': {'description': 'v component of wind (>0 = from south)','units': 'm/s', 'dtype':'float'},
            'w': {'description': 'w component of wind (>0 = upward)','units': 'm/s', 'dtype':'float'},
        }   