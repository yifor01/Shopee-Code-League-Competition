import pandas as pd
import datetime
from tqdm import tqdm
from p_tqdm import p_map
from functools import partial
df = pd.read_csv(r'./data/order_brush_order.csv')
df.event_time = pd.to_datetime(df.event_time)
df = df.sort_values(by=['shopid', 'event_time'])


def check(_shopid, _df=df):
    import datetime
    sub_df = _df[_df.shopid == _shopid]
    if len(sub_df) > 2:
        tar_userid = []
        for _time in sub_df.event_time:
            _target = sub_df[(sub_df.event_time >= _time) & (
                sub_df.event_time - _time <= datetime.timedelta(0, 3600, 0))]
            num_order1 = len(_target)
            num_user1 = len(set(_target.userid))
            if (num_order1 / num_user1) >= 3:
                most_count = min(max(_target.userid.value_counts()), 3)
                _tdf = _target.userid.value_counts().reset_index()
                tar_userid.extend(_tdf[_tdf.userid >= most_count]['index'])

        tar_userid = list(set(tar_userid))
        return '&'.join([str(x) for x in sorted(tar_userid)]
                        ) if tar_userid else '0'
    else:
        return '0'


check_p = partial(check, _df=df)
res111 = p_map(check_p, list(df.shopid.unique()), num_cpus=14)
output = pd.DataFrame({
    'shopid': df.shopid.unique(),
    'userid': res111
})

output.to_csv('result_6.csv', index=False)
