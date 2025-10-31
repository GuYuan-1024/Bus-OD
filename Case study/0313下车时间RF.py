import pandas as pd
from tqdm import tqdm
tqdm.pandas()

def method_1():
    df = pd.read_csv('/home/code/HYY/0109线路test/0130随机森林预测下车站点.csv',low_memory=False)
    df_arrive = pd.read_csv('arrive_data.csv',usecols=['busno','arrive_time','station_name'])
    df['enstation_time'] = pd.to_datetime(df['enstation_time'])
    df_arrive['arrive_time'] = pd.to_datetime(df_arrive['arrive_time'],format='mixed')

    out = []
    def ex_time(row):
        enstation_time = row['enstation_time']
        exstation_name = row['exstation_name']
        busno = row['busno']
        bus_ex = df_arrive[(df_arrive['busno'] == busno) & (df_arrive['station_name'] == exstation_name)]
        if bus_ex.empty is False:
            bus_ex['values'] = (bus_ex['arrive_time'] - enstation_time).dt.total_seconds() / 60
            try:
                bus_ex = bus_ex[bus_ex['values'] >= 0]
                min_index = bus_ex['values'].idxmin()
                min_value = float(bus_ex.loc[min_index, 'values'])
                exstation_time = bus_ex.loc[min_index,'arrive_time']
                if (3 <= min_value) & (min_value <= 100):
                    out.append(exstation_time)
                    return out
                else:
                    out.append(999)
                    return out
            except:
                out.append(999)
                return out
        else:
            out.append(999)
            return out

    df.progress_apply(ex_time,axis=1)
    df['exstation_time'] = out
    df.to_csv('test.csv',index=False)

def method_2(day):
    import warnings
    warnings.filterwarnings("ignore")
    df = pd.read_csv('0313RF下车站点.csv',low_memory=False)
    df_arrive = pd.read_csv('arrive_data.csv',usecols=['busno','arrive_time','station_name'])
    df['enstation_time'] = pd.to_datetime(df['enstation_time'])
    df_arrive['arrive_time'] = pd.to_datetime(df_arrive['arrive_time'],format='mixed')
    day = str(day)
    df = df[(pd.to_datetime('2022-11-' + day + ' 00:00:00') <= df['enstation_time']) & (df['enstation_time'] <= pd.to_datetime('2022-11-' + day + ' 23:59:59'))]
    df_arrive = df_arrive[(pd.to_datetime('2022-11-' + day + ' 00:00:00') <= df_arrive['arrive_time']) & (df_arrive['arrive_time'] <= pd.to_datetime('2022-11-' + day + ' 23:59:59'))]
    group = df.groupby(by=['busno','exstation_name'])
    for i,j in tqdm(group):
        busno = i[0]
        exstation_name = i[1]
        bus_ex = df_arrive[(df_arrive['busno'] == busno) & (df_arrive['station_name'] == exstation_name)]
        for en_time,index in zip(j['enstation_time'],j.index):
            bus_ex['values'] = (bus_ex['arrive_time'] - en_time).dt.total_seconds() / 60
            tem = bus_ex[bus_ex['values'] >= 0]
            if tem.empty == False:
                min_value = min(tem['values'])
                min_index = tem['values'].idxmin()
                exstation_time = tem.loc[min_index, 'arrive_time']
                if (3 <= min_value) & (min_value <= 100):
                    df.loc[index,'exstation_time'] = exstation_time
    df.to_csv('0227下车站点匹配/test_' + day + '.csv',index=False)


if __name__ == '__main__':
    import sys
    day = int(sys.argv[1])
    method_2(day)