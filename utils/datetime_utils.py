"""Module for datetime utilities."""

import datetime
import pytz
import dateutil

class DateTimeRange():
    def __init__(self,start_dt,end_dt,tz = 'utc'):
        self.tz = pytz.timezone(tz)

        self.start_dt = self._parse_datetime(start_dt)
        self.end_dt = self._parse_datetime(end_dt)

    def _parse_datetime(self,dt):
        if isinstance(dt, str):
            dt = dateutil.parser.parse(dt)
        
        if dt.tzinfo is None:
            dt = self.tz.localize(dt)
        else:
            if dt.tzinfo != self.tz:
                raise ValueError(f"Timezone of {dt} does not match the specified timezone {self.tz}")
        
        return dt
    
    def get_dates_in_range(self,fmt = None):
        dates = []
        current_date = self.start_dt
        while current_date <= self.end_dt:
            if fmt == None:
                dates.append(current_date.date())
            elif type(fmt) == str:
                dates.append(current_date.strftime(fmt))
            current_date += datetime.timedelta(days=1)
        return dates

    def new_tz(self,tz):
        if type(tz) == str:
            tz = pytz.timezone(tz)
        elif not isinstance(tz,pytz.tzinfo.BaseTzInfo):
            raise ValueError(f"Invalid timezone: {tz}")

        start_dt = self.start_dt.astimezone(tz)
        end_dt = self.end_dt.astimezone(tz)
        return DateTimeRange(start_dt,end_dt)
