# -*- coding: utf-8 -*-
"""
Created on Mon Apr 30 15:41:28 2018

@author: alpes
"""

import os
import pandas as pd

path = 'D:/GitHub/TDX/info_file_reader/info_file/'
def read_stock_base_info(path):
    df = pd.DataFrame(columns=['name', 'id'])
    f = open(path, 'rb')
    f.read(50)
    count = 0
    data = f.read(6)
    while(len(data) > 0):
        stock_id = data.decode()
        f.read(17)
        data = f.read(8)
        name = data.decode(encoding='gbk')
        df.loc[count] = [name, stock_id]
        count = count + 1
        f.read(283)
        data = f.read(6)
    f.close()
    return df

df = read_stock_base_info(path + 'shm.tnf')
df.to_csv('D:/sh.csv', index=False)
df = read_stock_base_info(path + 'szm.tnf')
df.to_csv('D:/sz.csv', index=False)