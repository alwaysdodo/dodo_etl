from datetime import datetime, timedelta
from itertools import chain

import pandas as pd

filename = '/Users/jongwony/Downloads/KakaoTalk_Chat_🏃롱텀두두 6회_2022-04-04-08-20-56.csv'
df = pd.read_csv(filename)
df['Date'] = df['Date'].astype('datetime64[ns]')
df.set_index('Date').groupby(pd.Grouper(freq='H'))['Message'].count().to_csv('ltdd6_hourly.csv')
df.set_index('Date').groupby(pd.Grouper(freq='D'))['Message'].count().to_csv('ltdd6_daily.csv')
df.set_index('Date').groupby(pd.Grouper(freq='W'))['Message'].count().to_csv('ltdd6_weekly.csv')

filename = 'KakaoTalk_Chat_롱텀두두 3회차🏃‍♀️🏃_2022-04-04-08-26-53.csv'
df = pd.read_csv(filename)
df['Date'] = df['Date'].astype('datetime64[ns]')
df.set_index('Date').groupby(pd.Grouper(freq='H'))['Message'].count().to_csv('ltdd3_hourly.csv')
df.set_index('Date').groupby(pd.Grouper(freq='D'))['Message'].count().to_csv('ltdd3_daily.csv')
df.set_index('Date').groupby(pd.Grouper(freq='W'))['Message'].count().to_csv('ltdd3_weekly.csv')

filename = '/Users/jongwony/Downloads/KakaoTalk_Chat_롱-텀 두두 2회 🎉_2022-04-04-08-27-06.csv'
df = pd.read_csv(filename)
df['Date'] = df['Date'].astype('datetime64[ns]')
df.set_index('Date').groupby(pd.Grouper(freq='H'))['Message'].count().to_csv('ltdd2_hourly.csv')
df.set_index('Date').groupby(pd.Grouper(freq='D'))['Message'].count().to_csv('ltdd2_daily.csv')
df.set_index('Date').groupby(pd.Grouper(freq='W'))['Message'].count().to_csv('ltdd2_weekly.csv')

filename = '/Users/jongwony/Downloads/KakaoTalk_Chat_롱텀두두 4회_2022-04-04-08-26-35.csv'
df = pd.read_csv(filename)
df['Date'] = df['Date'].astype('datetime64[ns]')
df.set_index('Date').groupby(pd.Grouper(freq='H'))['Message'].count().to_csv('ltdd4_hourly.csv')
df.set_index('Date').groupby(pd.Grouper(freq='D'))['Message'].count().to_csv('ltdd4_daily.csv')
df.set_index('Date').groupby(pd.Grouper(freq='W'))['Message'].count().to_csv('ltdd4_weekly.csv')

filename = '/Users/jongwony/Downloads/KakaoTalk_Chat_롱텀두두 5회차🍂🍁_2022-04-04-08-26-17.csv'
df = pd.read_csv(filename)
df['Date'] = df['Date'].astype('datetime64[ns]')
df.set_index('Date').groupby(pd.Grouper(freq='H'))['Message'].count().to_csv('ltdd5_hourly.csv')
df.set_index('Date').groupby(pd.Grouper(freq='D'))['Message'].count().to_csv('ltdd5_daily.csv')
df.set_index('Date').groupby(pd.Grouper(freq='W'))['Message'].count().to_csv('ltdd5_weekly.csv')


def concat():
    for y in 'hourly', 'daily', 'weekly':
        yield pd.concat([pd.read_csv(f'ltdd{x}_{y}.csv') for x in range(2, 7)])

round_data = [
    [1, 2, datetime(2021, 7, 18)],
    [2, 2, datetime(2021, 7, 25)],
    [3, 2, datetime(2021, 8, 1)],
    [1, 3, datetime(2021, 8, 22)],
    [2, 3, datetime(2021, 8, 29)],
    [3, 3, datetime(2021, 9, 5)],
    [1, 4, datetime(2021, 10, 17)],
    [2, 4, datetime(2021, 10, 24)],
    [3, 4, datetime(2021, 10, 31)],
    [1, 5, datetime(2021, 11, 21)],
    [2, 5, datetime(2021, 11, 28)],
    [3, 5, datetime(2021, 12, 5)],
    [1, 6, datetime(2022, 2, 27)],
    [2, 6, datetime(2022, 3, 6)],
    [3, 6, datetime(2022, 3, 13)],
]

hourly, daily, weekly = [df for df in concat()]

hourly['Date'] = hourly['Date'].astype('datetime64[ns]')
hourly['hour'] = hourly['Date'].dt.hour
hourly['index'] = 0
hourly['round'] = 0
for i, r, end in round_data:
    round_part = (end - timedelta(days=6) <= hourly['Date']) & (hourly['Date'] <= end)
    hourly.loc[round_part, 'index'] = i
    hourly.loc[round_part, 'round'] = r
hours = hourly[hourly['index'] != 0][['index', 'round', 'hour', 'Message', 'Date']]

daily['Date'] = daily['Date'].astype('datetime64[ns]')
daily['weekday'] = daily['Date'].dt.weekday
daily['index'] = 0
daily['round'] = 0
for i, r, end in round_data:
    round_part = (end - timedelta(days=6) <= daily['Date']) & (daily['Date'] <= end)
    daily.loc[round_part, 'index'] = i
    daily.loc[round_part, 'round'] = r
days = daily[daily['index'] != 0][['index', 'round', 'weekday', 'Message', 'Date']]

weeks = weekly[(1 <= weekly.index) & (weekly.index <= 3)].reset_index()
weeks['round'] = list(chain(*([i] * 3 for i in range(2, 7))))
weeks = weeks[['index', 'round', 'Message', 'Date']]
