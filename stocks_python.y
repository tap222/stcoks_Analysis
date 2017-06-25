import os
import pandas as pd
os.chdir('/home/hduser1/Downloads/stocks_project/')
os.listdir(".")
df = pd.read_csv('POWERGRID.csv')
df.columns = ['Symbol','Date','time','Open','High','Low','Close','Volume']
df['Date'] = pd.to_datetime(df.Date)
df['day'] = df.Date.dt.day
df['month'] = df.Date.dt.month
df_day = df.groupby('day',as_index = False)['Volume'].sum()
df = pd.merge(df,df_day,on='day',how='left')
Day = []
Volume = []
Close_Price = []
Month = []
for i in range(len(df)-1):
       row1,row2 = df.iloc[i],df.iloc[i+1]
       if((row2['day'] != row1['day']) and  (row2['month'] == 5)):
           Day.append(row1['day'])
           Month.append(row1['month'])
           Volume.append(row1['Volume_y']/75)
           Close_Price.append(row1['Close'])
       else:
           if((row2['day'] != row1['day']) and  (row2['month'] ==6)):
               Day.append(row1['day'])
               Month.append(row1['month'])
               Volume.append(row1['Volume_y']/75)
               Close_Price.append(row1['Close'])
           else:
               continue

Day = pd.DataFrame(Day, columns = ['Day'])
Month = pd.DataFrame(Month, columns = ['Month'])
Volume = pd.DataFrame(Volume ,columns = ['Volume'])
Close_Price = pd.DataFrame(Close_Price, columns = ['Close_Price'])
comb = pd.concat([Day,Month,Volume,Close_Price],axis=1)
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import os
os.chdir('/home/hduser1/Downloads/stocks_project/')
import urllib,time,datetime
import  pandas as pd
class Quote(object):
  
  DATE_FMT = '%Y-%m-%d'
  TIME_FMT = '%H:%M:%S'
  
  def __init__(self):
    self.symbol = ''
    self.date,self.time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(7))

  def append(self,dt,open_,high,low,close,volume):
    self.date.append(dt.date())
    self.time.append(dt.time())
    self.open_.append(float(open_))
    self.high.append(float(high))
    self.low.append(float(low))
    self.close.append(float(close))
    self.volume.append(int(volume))

 # schedule.every(1).http://minutes.do(fetchData) 
    
  def to_csv(self):
    return ''.join(["{0},{1},{2},{3:.2f},{4:.2f},{5:.2f},{6:.2f},{7}\n".format(self.symbol,
              self.date[bar].strftime('%Y-%m-%d'),self.time[bar].strftime('%H:%M:%S'),
              self.open_[bar],self.high[bar],self.low[bar],self.close[bar],self.volume[bar]) 
              for bar in xrange(len(self.close))])
    
  def write_csv(self,filename):
    with open(filename,'w') as f:
      f.write(self.to_csv())
        
  def read_csv(self,filename):
    self.symbol = ''
    self.date,self.time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(7))
    for line in open(filename,'r'):
      symbol,ds,ts,open_,high,low,close,volume = line.rstrip().split(',')
      self.symbol = symbol
      dt = datetime.datetime.strptime(ds+' '+ts,self.DATE_FMT+' '+self.TIME_FMT)
      self.append(dt,open_,high,low,close,volume)
    return True

  def __repr__(self):
    return self.to_csv()

class GoogleIntradayQuote(Quote):
  ''' Intraday quotes from Google. Specify interval seconds and number of days '''
  def __init__(self,symbol,interval_seconds=300,num_days=5):
    super(GoogleIntradayQuote,self).__init__()
    self.symbol = symbol.upper()
    url_string = "http://www.google.com/finance/getprices?q={0}".format(self.symbol)
    url_string += "&x=NSE&i={0}&p={1}d&f=d,o,h,l,c,v".format(interval_seconds,num_days)
    csv = urllib.urlopen(url_string).readlines()
    for bar in xrange(7,len(csv)):
      if csv[bar].count(',')!=5: continue
      offset,close,high,low,open_,volume = csv[bar].split(',')
      if offset[0]=='a':
        day = float(offset[1:])
        offset = 0
      else:
        offset = float(offset)
      open_,high,low,close = [float(x) for x in [open_,high,low,close]]
      dt = datetime.datetime.fromtimestamp(day+(interval_seconds*offset))
      self.append(dt,open_,high,low,close,volume)
   
   
if __name__ == '__main__':
  stocks = GoogleIntradayQuote('POWERGRID',300,1)
  #print q                                           # print it out
  stocks.write_csv('POWERGRID_new.csv')




df_new = pd.read_csv('POWERGRID_new.csv')




#dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')  
df_new = pd.read_csv('POWERGRID_new.csv',sep=',',header=None)
df_new.columns = ['Symbol','Date','time','Open','High','Low','Close','Volume']
df_new = df_new.iloc[0:1]
comb_last = comb.iloc[comb.shape[0]-1]
comb_last = comb_last['Close_Price']
comb_mean = comb['Volume'].mean()
df_new['Ref_volume']= comb_mean
df_new = pd.DataFrame(df_new)
df_new['last_price'] = comb_last
df_new['pct_chng'] = ((df_new['Close']-df_new['last_price'])/df_new['last_price'])*100
signal_volume = 0
for i in range(len(df_new)-1):
    if(df_new['Volume'] - 4 * df_new['Ref_Volume']  >= 0):
        if(df_new['pct_chng'] >= 0):
            signal_volume = 0.4 * 1
            sys.exit()
        else:
            if(df_new['pct_chng'] < 0):
                signal_volume = -0.4 * 1
            sys.exit()            
    else:
        if (df_new['Volume'] - 3 * df_new['volume'] >= 0):
            if(df_new['pct_chng'] >= 0):
                signal_volume = 0.4 * 0.85
                sys.exit()
            else:
                if(df_new['pct_chng'] < 0):
                    signal_volume = -0.4 * 0.85
                sys.exit()                    
        else:
            if(df_new['Volume'] - 2 * df_new['volume'] >= 0):
                if(df_new['pct_chng'] >= 0):
                    signal_volume = 0.4 * 0.75
                    sys.exit()
                else:
                    if(df_new['pct_chng'] < 0):
                        signal_volume = 0.4 * 0.75
                        sys.exit()
            else:
                signal_volume = 0
    #continue

signal_pct_chng = 0
for i in range(len(df_new)-1):
    if(abs(df_new['pct_chng']) >= 4):
        if(df_new['pct_chng'] >= 0):
            signal_pct_chng = 0.2 * 0.95
            sys.exit()
        else:
            if(df_new['pct_chng'] < 0):
                signal = -0.20 * 0.50
                sys.exit()
    else:
        if(abs(df_new['pct_chng']) >= 3):
            if(df_new['pct_chng'] >= 0):
                signal_pct_chng = 0.20 * 0.75
                sys.exit()
            else:
                if(df_new['pct_chng'] < 0):
                    signal_pct_chng = -0.20 * 0.75
                    sys.exit()
        else:
            if(abs(df_new['pct_chng']) >= 2):
                if(df_new['pct_chng'] >= 0):
                    signal_pct_chng = 0.20*0.70
                    sys.exit()
                else:
                    if(df_new['pct_chng'] < 0):
                        signal_pct_chng = -0.20*70
                        sys.exit()
            else:
                if(abs(df_new['pct_chng']) >= 1):
                    if(df_new['pct_chng'] >= 0):
                        signal_pct_chng = 0.20*0.5
                        sys.exit()
                    else:
                        if(df_new['pct_chng'] < 0):
                            signal_pct_chng = -0.20 * 0.50
                            sys.exit()
                else:
                    signal_pct_chng = 0
                        
