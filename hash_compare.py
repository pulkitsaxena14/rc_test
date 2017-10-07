import pandas as pd
from sqlalchemy import *
import hashlib
import numpy
from psycopg2.extensions import register_adapter, AsIs
def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(numpy.int64, adapt_numpy_int64)


engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
connection = engine.connect()
meta = MetaData(bind=engine)
m = hashlib.md5()
zeta_wallet = Table('zeta_wallet', meta,
                    Column('wallet_id', INTEGER, Sequence('wallet_id_seq'), primary_key=True),
                    Column('DDate', TEXT, nullable=True),
                    Column('From_to_party', TEXT, nullable=True),
                    Column('Credit', FLOAT, nullable=True),
                    Column('Debit', FLOAT, nullable=True),
                    Column('Balance', FLOAT, nullable=True),
                    Column('Bank_ref_txn_id', TEXT, nullable=True),
                    Column('Load', FLOAT, nullable=True),
                    Column('Key', TEXT, nullable=True),
                    Column('Lender', TEXT, nullable=True),
                    Column('Zeta_Order_Id', TEXT, nullable=True),
                    Column('DMI_User_bal', FLOAT, nullable=True),
                    Column('RC_User_bal', FLOAT, nullable=True),
                    Column('DMI_usr_bal_post_may', FLOAT, nullable=True),
                    Column('Chandan_comment', TEXT, nullable=True),
                    Column('Current_date', TEXT, nullable=True),
                    Column('Status', TEXT, nullable=True),
                    Column('Unnamed_col1', FLOAT, nullable=True),
                    Column('Unnamed_col2', FLOAT, nullable=True),
                    Column('Unnamed_col3', FLOAT, nullable=True),
                    Column('Unnamed_col4', FLOAT, nullable=True),
                    Column('Unnamed_col5', FLOAT, nullable=True),
                    Column('Unnamed_col6', FLOAT, nullable=True),
                    Column('Unnamed_col7', FLOAT, nullable=True),
                    Column('Unnamed_col8', FLOAT, nullable=True),
                    Column('Unnamed_col9', FLOAT, nullable=True),
                    Column('Actual_bal', FLOAT, nullable=True),
                    Column('Data_status',TEXT, nullable=True),
                    Column('data_hash', TEXT, nullable=False))

engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
connection = engine.connect()

df1 = pd.read_csv('/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Loads_Zeta_Loads_mod.csv')

if engine.has_table('zeta_wallet') is False:
    zeta_wallet.create()
    ins = zeta_wallet.insert()
    for tup in df1.itertuples():
        hash_obj = ''
        for i in range(1, len(df1.columns)+1):

            hash_obj += str(tup[i])
        hash_code = hashlib.md5(hash_obj.encode()).hexdigest()
        engine.execute(zeta_wallet.insert(), DDate=tup[1], From_to_party=tup[2], Credit=tup[3], Debit=tup[4],
                       Balance=tup[5], Bank_ref_txn_id=tup[6], Load=tup[7], Key=tup[8], Lender=tup[9],
                       Zeta_Order_Id=tup[10], DMI_User_bal=tup[11], RC_User_bal=tup[12], DMI_usr_bal_post_may=tup[13],
                       Chandan_comment=tup[14], Current_date=tup[15], Status=tup[16], Unnamed_col1=tup[17],
                       Unnamed_col2=tup[18], Unnamed_col3=tup[19], Unnamed_col4=tup[20], Unnamed_col5=tup[21],
                       Unnamed_col6=tup[22], Unnamed_col7=tup[23], Unnamed_col8=tup[24], Unnamed_col9=tup[25],
                       Actual_bal=tup[26], Data_status='NEW', data_hash=hash_code)

df2 = pd.read_sql_table('zeta_wallet', engine)

hash_df1 = []
for tup in df1.itertuples():
    hash_obj = ''
    for i in range(1, len(df1.columns) + 1):
        hash_obj += str(tup[i])
    hash_code = hashlib.md5(hash_obj.encode()).hexdigest()
    hash_df1.append(hash_code)

df2_hash = pd.DataFrame(df2['data_hash'].copy())
df1_hash = pd.DataFrame(hash_df1)
for j, row in enumerate(df2_hash.itertuples()):
    for k, cow in enumerate(df1_hash.itertuples()):
        flag = 0
        if row[1] == cow[1]:
            df2.set_value(j, 'Data_status', 'DELETED')
            break

val = df2.loc[df2['Data_status'] == 'DELETED', 'wallet_id'].values
#tmp = df2.loc[df2['Data_status'] == 'NEW', 'wallet_id'].values
stmt = zeta_wallet.update().where(zeta_wallet.c.wallet_id == bindparam('wlt_id')).values(Data_status='DELETED')
st = []
for i in val:
    st.append({'wlt_id': i})
connection.execute(stmt, st)
ins = zeta_wallet.insert()
for tup in df1.itertuples():
    hash_obj = ''
    for i in range(1, len(df1.columns) + 1):
        hash_obj += str(tup[i])
    hash_code = hashlib.md5(hash_obj.encode()).hexdigest()
    engine.execute(zeta_wallet.insert(), DDate=tup[1], From_to_party=tup[2], Credit=tup[3], Debit=tup[4],
                   Balance=tup[5], Bank_ref_txn_id=tup[6], Load=tup[7], Key=tup[8], Lender=tup[9],
                   Zeta_Order_Id=tup[10], DMI_User_bal=tup[11], RC_User_bal=tup[12], DMI_usr_bal_post_may=tup[13],
                   Chandan_comment=tup[14], Current_date=tup[15], Status=tup[16], Unnamed_col1=tup[17],
                   Unnamed_col2=tup[18], Unnamed_col3=tup[19], Unnamed_col4=tup[20], Unnamed_col5=tup[21],
                   Unnamed_col6=tup[22], Unnamed_col7=tup[23], Unnamed_col8=tup[24], Unnamed_col9=tup[25],
                   Actual_bal=tup[26], Data_status='NEW', data_hash=hash_code)

connection.close()