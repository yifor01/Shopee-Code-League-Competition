import pandas as pd
import re
from tqdm import tqdm
import datetime
import pytz

pd.options.display.max_columns = 10

df = pd.read_csv(r'./Competition5/delivery_orders_march.csv')

def addrees_map(address):
    if re.search('Manila',address,re.I):
        return "Metro Manila"
    elif re.search('Luzon',address,re.I):
        return "Luzon"
    elif re.search('Visayas',address,re.I):
        return "Visayas"
    elif re.search('Mindanao',address,re.I):
        return "Mindanao"
    else:
        return 'error'

def time_cost(buy,sell):
    if (buy=='Metro Manila') and (sell=='Metro Manila'):
        return 3
    elif (buy=='Metro Manila') and (sell=='Luzon'):
        return 5
    elif (sell=='Metro Manila') and (buy=='Luzon'):
        return 5
    elif (buy=='Luzon') and (sell=='Luzon'):
        return 5
    else:
        return 7

def compute_late(time0,time1,time2):
    if time1>time0:
        return 1
    elif time2>3:
        return 1
    else:
        return 0

def holiday_shift(day0,day2):
    day2 = day0++datetime.timedelta(days=day2)
    return int((day0.month, day0.day) < (3, 25) < (day2.month, day2.day))+\
           int((day0.month, day0.day) < (3, 30) < (day2.month, day2.day))+\
           int((day0.month, day0.day) < (3, 31) < (day2.month, day2.day))

weekday_map = {
    'Monday':1,
    'Tuesday':2,
    'Wednesday': 3,
    'Thursday': 4,
    'Friday': 5,
    'Saturday': 6,
    'Sunday': 7
}

# change address name
df['addres_res_buy']  = df['buyeraddress'].apply(lambda x: addrees_map(x))
df['addres_res_sell'] = df['selleraddress'].apply(lambda x: addrees_map(x))

# compute working time
work_time = []
for x,y in df[['addres_res_buy','addres_res_buy']].values:
    work_time.append(time_cost(x,y))
df['work_time'] = work_time

# change realtime
tw = pytz.timezone('Asia/Taipei')
data = df[['orderid', 'pick', '1st_deliver_attempt', '2nd_deliver_attempt','work_time']].copy()
data['0_realtime'] = data['pick'].apply(lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=tw).date() )
data['1_realtime'] = data['1st_deliver_attempt'].apply(lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=tw).date() )
data['2nd_deliver_attempt'] = data['2nd_deliver_attempt'].fillna(data['1st_deliver_attempt'])
data['2_realtime'] = data['2nd_deliver_attempt'].apply(lambda x:datetime.datetime.fromtimestamp(x).replace(tzinfo=tw).date())


# compute holiday shift
data['0_weekday'] = data['pick'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime("%A") ).map(weekday_map)

shift = []
for x,y in data[['0_realtime','work_time']].values:
    shift.append(holiday_shift(x,y))


data['work_time_fix'] = data['work_time']+ ((data['work_time']+data['0_weekday'])>=7).astype('int') +shift


data['use_time1'] = (data['1_realtime'] -data['0_realtime']).dt.days
data['use_time2'] = (data['2_realtime'] -data['1_realtime']).dt.days


# compute islate
y = []
for t0,t1,t2 in data[['work_time_fix','use_time1','use_time2']].values:
    y.append(compute_late(t0,t1,t2))

data['is_late'] = y
data[['orderid','is_late']].to_csv(r'./Competition5/test10.csv',index = False)

