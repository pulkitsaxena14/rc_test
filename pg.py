from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
c = engine.connect()
df1 = pd.read_csv('/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Loads_Zeta_Loads.csv')
df1.to_sql('zeta_wallet', engine, if_exists='append', index=False)
c.close()