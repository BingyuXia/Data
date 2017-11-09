import sys
import scipy.io as sio
import pandas as pd
sys.path.append("H:\\MinData20170907")

df_columns = ["ID", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "Value"]
df = pd.DataFrame(sio.loadmat("000060.mat")["data1"], columns=df_columns)
