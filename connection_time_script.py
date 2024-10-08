
import datetime
import time
from django.db import connections
from IngestionEngine.models import SourceData


def print_log(start_time):
    with open(log_file, 'a') as f:
        out = f"Alive after {datetime.datetime.now() - start_time} at: {datetime.datetime.now()}\n"
        print(out)
        f.write(out)


def print_error(start_time):
    with open(log_file, 'a') as f:
        out = f"Failed before {datetime.datetime.now() - start_time} at: {datetime.datetime.now()}\n"
        print(out)
        f.write(out)


start_time = datetime.datetime.now()
data = SourceData.objects.first()
db_conn = connections['default']

log_file = './log.log'

print_log(start_time)

for i in [60, 60*60, 60*60*2, 60*60*4]:
    time.sleep(i)

    try:
        c = db_conn.cursor()
    except Exception as e:
        print_error(start_time)
    else:
        print_log(start_time)
