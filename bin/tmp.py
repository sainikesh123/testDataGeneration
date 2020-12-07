import pandas as pd
import re
df=pd.read_excel('../output/GB_staffSavingsAccount_data.xlsx')
#df.To_Date= df.To_Date.apply(lambda x: x.date())
lst=[x.to_pydatetime() for x in df.From_Date.tolist()]
print(type(df.To_Date.tolist()[0]))
print(type(lst[1005]))
