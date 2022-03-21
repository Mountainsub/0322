#!/usr/bin/env python
# coding: utf-8
import warnings
from tables import NaturalNameWarning
warnings.filterwarnings('ignore', category=NaturalNameWarning)

import pandas as pd
import numpy as np
import datetime
import threading
from concurrent import futures
import logging
import sys
sys.path.append("..")
from lib.ddeclient import DDEClient
import itertools

import time
class LastNPerfTime:
    def __init__(self, n):
        """
        過去n回の実行時間の合計を尺取り法で記録する
        """
        self.n = n
        self.count = 0
        self.sum_time = 0
        self.times = np.zeros(n)
        
        
    def start(self):
        """
        実行時間の計測を開始する
        """
        self.start_time = time.perf_counter() # timeより正確
        
    def end(self):
        """
        実行時間の計測を終了する
        """
        dtime = time.perf_counter() - self.start_time
        idx = self.count % self.n # self.nが2^xならここをビット論理積にできる
        time_n_before = self.times[idx]
        self.times[idx] = dtime
        self.count += 1
        self.sum_time += (dtime - time_n_before)
        
    def get_sum_time(self):
        """
        過去n回の実行時間を合計した値を返す
        """
        return self.sum_time

class ClientHolder(threading.Thread):
    def __init__(self, idx, codes, hdffoldername = "./data/"):
        """
        RSSサーバーに接続し、継続的に複数の銘柄の株価を取得する
        
        Parameters
        ----------
        idx: int
        ClientHolderにつける番号
        番号がかぶると同じファイルに書き込むことになる
        
        codes: array_like
        RSSサーバーにリクエストを送る銘柄のコード番号を格納したリスト
        
        """
        hdffilename = hdffoldername + str(idx).zfill(3) + ".hdf5" # 文字列・数値をゼロパディング（ゼロ埋め）するzfill()
        
        self.idx = idx
        self.clients = {}
        self.activate = {}
        self.close_value = "現在値" #price_request_str
        self.codes = codes
        self.codes_attrsafe = 'code_' + np.array(codes).astype('object') # pandasを使ってhdfを作るとき、数字から始まる列名にできない
        try:
            with pd.HDFStore(self.hdffilename) as store:
                self.df = store["check/"+str(idx)]
        except:
            self.df = pd.DataFrame.from_dict(data={i: [0] for i in codes}, orient="index",columns=["activate"])
            pass
        #print(len(codes))
        # RSSサーバーに接続し、127個のDDEClientを作る
        self.connect_all()
        
        # データ保存用のファイルを開く
        self.hdffilename = hdffilename
        self.store = pd.HDFStore(hdffilename)
        self.key_name = "classidx_" + str(self.idx) 
        
        
        # hdfにデータを追加する際のkeyを一つ適当なものに決める
        # keyを更新ごとに変えるとファイル容量が大変なことになる
        
        
        
    def connect_all(self):
        """
        RSSサーバーに接続する
        """
        for code in self.codes:
            try:
                self.clients[code] = DDEClient("rss", code)
            except Exception as e:
                print(f"an error occurred at code: {code} while connecting server.")
                pass
            else:
                df = self.df
                #df.loc[code, "activate"] = 0
                #raise Exception(e)
    
    def stop(self):
        t2= 0
        t1 = time.time()
        while t2 -t1 > 1:
            t2= time.time()
        else:
            return

    
    
    def get_price(self, code):
        """
        1つの銘柄の株価を取得する
        """ 
        client = self.clients[code]
        t1 = time.time()
        print(self.df["activate"].sum())
        if float(self.df["activate"].sum()) > 16:
            print(code,"waiting...")
            
                
                
                
        if float(self.df["activate"].sum()) <= 16:
            try:
                price = client.request("現在値").decode("sjis")
                #price = float(t1.run())
            except Exception as e:
                print(f"an error occurred at code: {code} while requesting")
                raise Exception(e)
            
            else:
                self.df.loc[code,"activate"] = 1
                with pd.HDFStore(self.hdffilename) as store:
                    store.put("check/"+str(self.idx), self.df)
                #self.store.append(self.key_name, pd.DataFrame([data_dict]))           
                #print(price)
                # korejyadame print(len(self.clients))
                #client.__del__()
                #print(code)
        else:
            t2 = time.time()
            while t2 -t1 < 1:
                t2= time.time()
                pass
            self.get_price(code)      

                
                #client.__init__("rss", code)
        return price
    
    """
    1. 初期化が終わって初めてのrequestを送ってから切断するまでの状態(Xとする)にあるddeclientが、RSSサーバーあたり同時に300個以上あってはいけない
    2. プロセスあたりddeclientは127個まで
    3. プロセス間で情報をリアルタイムかつ高速に共有するのは難しい
    よって、最大127個のddeclientを持つプログラムを各プロセスで走らせて、
    それぞれのプログラムでは状態Xのddeclientが300/18=16個（全体の約1/8）以下になるようにすることになります。
    それぞれの銘柄において、初期化→request→切断→初期化→…　とループを回せば、状態Xのddeclientは平均して10個程度になると思われるので、
    条件をクリアできるかなと思いこのアルゴリズムを提案した次第です。
    """

    def init2(self, client, code):
        client.__init__("rss", code)
    
    def delete(self, pre_code):
        client = self.clients[pre_code]
        client.__del__()
        self.df.loc[pre_code,"activate"] = 0
        self.init2(client, pre_code)
        #client.__init__("rss", pre_code)
        
        return
    def getting(self,prices, temps,k):
        for i, code in enumerate(self.codes, start=k):
            p=self.get_price(self.codes[i])
            if int(float(p)) == 0:
                num = i
                break
            prices[self.codes_attrsafe[i]] = p
            try:
                pre_code = temps[-1]
            except Exception:
                pass
            else:
                self.delete(pre_code)
                pass
            
            temps.append(code)
            num=0
        return [prices,temps,num]
    
    
    def get_prices(self):
        """
        複数の銘柄の株価を取得し、保存する
        """
        temps =[]
        prices = {}
        #prices['time_start'] = np.datetime64(datetime.datetime.now())
        t = np.datetime64(datetime.datetime.now())
        Firststep = True

        if True: #ここ
            
            if Firststep:
                temp = self.getting({},[],0)
                Firststep = False
            elif temp[2] < 125:
                prices, temps,num = temp[0],temp[1], temp[2]
                temp = self.getting(prices,temps,num)
            else:
                pass
        prices = temp[0]
        """
        else:
            prices, temps,num = temp[0],temps[1], temps[2]
        """

        prices['time_start'] = t
        prices['time_end'] = np.datetime64(datetime.datetime.now())
        self.save(prices)
        return prices


    def save(self, data_dict):
        """
        取得した株価を保存する
        """
        self.store.append(self.key_name, pd.DataFrame([data_dict]))
        
    def get_prices_forever(self):
        """
        継続的に株価を取得して保存し続ける
        """
        while True:
            try:
                self.get_prices()
            except KeyboardInterrupt:
                break
            except Exception as e:
                raise Exception(e)

    def calc(self,prices, weights):
        num = 0
        #prices=itertools.chain.from_iterable(prices)
        for i, code in enumerate(self.codes):
            num += float(prices[self.codes_attrsafe[i]])* float(weights[i]) 
        return num

if __name__ == '__main__':
    idx = int(sys.argv[1])
    foldername = sys.argv[2]
    codes = sys.argv[3:]
    holder = ClientHolder(idx, codes, foldername)
    
    holder.get_prices_forever()