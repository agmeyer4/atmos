"""Module for datetime utilities.

Classes:
    DateTimeRange: Class for handling datetime ranges.

"""

import datetime
import pytz
import dateutil 

class DateTimeRange():
    """Class for handling datetime ranges.

    Attributes:
        start_dt (datetime.datetime): Start of the datetime range.
        end_dt (datetime.datetime): End of the datetime range.
        tz (pytz.tzinfo.BaseTzInfo): Timezone of the datetime range.

    """

    def __init__(self,start_dt,end_dt,tz = 'UTC'):
        self.tz = pytz.timezone(tz)

        self.start_dt = self._parse_datetime(start_dt)
        self.end_dt = self._parse_datetime(end_dt)

    def _parse_datetime(self,dt):
        """Parses a datetime object or string and returns a datetime object with the correct timezone.
        
        Args:
            dt (datetime.datetime or str): Datetime object or string to be parsed.
            
        Returns:
            datetime.datetime: Datetime object with the correct timezone.

        Raises:
            ValueError: If the timezone of the datetime object does not match the specified timezone.
        """

        if isinstance(dt, str): #Check if it's a string
            dt = dateutil.parser.parse(dt) #Parse the string
        
        if dt.tzinfo is None: #If the datetime object has no timezone
            dt = self.tz.localize(dt) #Add the timezone
        else:
            if dt.tzinfo != self.tz: #If the timezone of the datetime object does not match the specified timezone
                raise ValueError(f"Timezone of {dt} does not match the specified timezone {self.tz}") #Raise an error
        return dt
    
    def get_dates_in_range(self,fmt = None):
        """
        Returns a list of dates in the datetime range.
        
        Args:
            fmt (str): Format of the dates. If None, the dates will be returned as datetime.date objects.
            
        Returns:
            list: List of dates in the datetime range.
        """

        dates = [] #List to store the dates
        current_date = self.start_dt #Start from the start date
        while current_date <= self.end_dt: #While the current date is less than or equal to the end date
            if fmt == None: #If no format is specified
                dates.append(current_date.date()) #Append the date as a datetime.date object
            elif type(fmt) == str: #If a format is specified
                dates.append(current_date.strftime(fmt)) #Append the date as a string with the specified format
            current_date += datetime.timedelta(days=1) #Increment the current date by one day
        return dates

    def new_tz(self,tz):
        """
        Returns a new DateTimeRange object with a different timezone.
        
        Args:
            tz (str or pytz.tzinfo.BaseTzInfo): New timezone.
            
        Returns:
            DateTimeRange: New DateTimeRange object with the new timezone.

        Raises:
            ValueError: If the timezone is not a string or a timezone object.
        """

        if type(tz) == str: #If the timezone is a string
            tz = pytz.timezone(tz) #Get the timezone object
        elif not isinstance(tz,pytz.tzinfo.BaseTzInfo): #If the timezone is not a string or a timezone object
            raise ValueError(f"Invalid timezone: {tz}") #Raise an error

        start_dt = self.start_dt.astimezone(tz) #Convert the start date to the new timezone
        end_dt = self.end_dt.astimezone(tz) #Convert the end date to the new timezone
        return DateTimeRange(start_dt,end_dt) #Return a new DateTimeRange object with the new timezone
