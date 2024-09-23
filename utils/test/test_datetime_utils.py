import os 
import sys
import datetime
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
import datetime_utils

def test_DateTimeRange():
    dtr = datetime_utils.DateTimeRange(datetime.datetime(2021,1,1),datetime.datetime(2021,1,2))
    assert dtr.__dict__ == {'tz': datetime.timezone.utc, 'start_dt': datetime.datetime(2021,1,1,0,0), 
                            'end_dt': datetime.datetime(2021,1,2,0,0)}


def main():
    dtr = datetime_utils.DateTimeRange(datetime.datetime(2021,1,1),datetime.datetime(2021,1,2))
    print(dtr.__dict__)
    #test_DateTimeRange()

if __name__ == "__main__":
    main()