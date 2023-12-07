# import pandas as pd
from datetime import datetime, timedelta
import pandas as pd

# df = pd.DataFrame(columns=['Date'], data=['1672542000'])
# # df['Date'] = df['Date'].astype(str)
# print(df)
# df['Date'] = pd.to_datetime(df['Date'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kuala_Lumpur')
# print(df)
# # print(start)
# # rng = pd.date_range(start, periods=10)

# # df = pd.DataFrame({'Date': rng, 'a': range(10)})

# # df.Date = df.Date.dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
# # print (df)

# # endOfDate = datetime.today().date() 
# endOfDate = pd.to_datetime(datetime.today())
# startOfDate = endOfDate - pd.Timedelta(days=30)
# print("Start Of Date: " + str(startOfDate) + "\n" + "End Of Date: " + str(endOfDate))

# import time
# start_time = time.time()
# end_time = time.time() - start_time
# final_end_time = end_time / 60
# print("--- %s minutes ---" % final_end_time)

