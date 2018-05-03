# -*- coding: utf-8 -*-
"""
Created on Wed May  2 08:02:56 2018

@author: Administrator
"""

import pandas as pd
import os
import numpy as np
import operator as op

path = 'D:/new_tdx/T0002/hq_cache/'

class info_reader:
    
    def __init__(self, path, refresh=True):
        if (os.path.isfile('./info.csv') == True and refresh == False):
            self.__info = pd.read_csv('./info.csv', encoding='gbk')
            self.__info = self.__info.set_index('stock_id')
            return
        
        name_id = self.__read_base_info(path)
        #name_id.to_csv('./name_id.csv', index=False)
        stocks_in_block = self.__read_block_file(path + 'block_gn.dat')
        stocks_in_block.update(self.__read_block_file(path + 'block_zs.dat'))
        stocks_in_block.update(self.__read_block_file(path + 'block_fg.dat'))
        column_name = list(stocks_in_block.keys())
        
        stocks = name_id[(name_id['stock_id']>0) & (name_id['stock_id']<20000) & (name_id['market']=='sz')]
        stocks = stocks.append(name_id[(name_id['stock_id']>=300000) & (name_id['stock_id']<310000) & (name_id['market']=='sz')])
        stocks = stocks.append(name_id[(name_id['stock_id']>=600000) & (name_id['stock_id']<700000) & (name_id['market']=='sh')])
        #stocks.to_csv('./stocks.csv', index=False)
        self.__info = pd.DataFrame(index=stocks['stock_id'], columns=column_name, dtype=np.int8)
        self.__info = self.__info.fillna(0)
        for i in range(len(column_name)):
            self.__info.loc[stocks_in_block[column_name[i]], column_name[i]] = 1
        self.__info.to_csv('./info.csv')

    def __get_file_size(self, file):
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0, os.SEEK_SET)
        return file_size
    
    def __read_all(self, file_name):
        file = open(file_name, 'rb')
        file_size = self.__get_file_size(file)
        data = file.read(file_size)
        file.close()
        return data, file_size
    
    def __read_tnf_file(self, file_name, market):
        df = pd.DataFrame(columns=['stock_id', 'name', 'market'])    
        data, file_size = self.__read_all(file_name)    
        offset = 50
        count = 0
        
        while(offset != file_size):        
            stock_id = data[offset : offset + 6]       
            offset = offset + 6
            stock_id = int(stock_id.decode())
            offset = offset + 17
            
            name_count = 0
            for i in range(8):
                if(data[offset + i] != 0):
                    name_count = name_count + 1
                else:
                    break
            name = data[offset : offset + name_count].decode(encoding='gbk')
            df.loc[count] = [stock_id, name, market]
            offset = offset + 291
            count = count + 1
            
            #print(count)
            
        return df
    
    def __read_base_info(self, path):
        return self.__read_tnf_file(path + 'shm.tnf', 'sh').append(self.__read_tnf_file(path + 'szm.tnf', 'sz'))
    
    def __read_block_file(self, file_name):
        data, file_size = self.__read_all(file_name)
        block_info = {}
        offset = 386
             
        while(offset != file_size):
            stocks = []
            name_count = 0
            for i in range(8):
                if(data[offset + i] != 0):
                    name_count = name_count + 1
                else:
                    break
            block_name = data[offset : offset + name_count]
            block_name = block_name.decode(encoding='gbk')
            if(op.eq(block_name, '精选指数') == True):
                offset = (int((offset - 386) / 2813) + 1) * 2813 + 386
                continue
            offset = offset + 13
            while(data[offset] >= 0x30 and data[offset] <= 0x39):
                sid = int(data[offset : offset + 6].decode())
                if (sid < 700000):
                    stocks.append(sid)
                offset = offset + 7
            block_info[block_name] = stocks
            offset = (int((offset - 386) / 2813) + 1) * 2813 + 386
        
        return block_info
    
    def get_info(self):
        #ret = pd.DataFrame()
        ret = self.__info.copy(deep=True)
        return ret
    
    def get_blocks_of_stock(self, stock_id):
        return self.__info.loc[[stock_id], ['%s' % col for col in self.__info.columns.tolist()  if self.__info.at[stock_id, col] == 1]].columns.tolist()


reader = info_reader(path, refresh=False) 
info = reader.get_info()
#info.to_csv('./info.csv')

