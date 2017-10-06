import pandas as pd
from sqlalchemy import *
import numpy
from psycopg2.extensions import register_adapter, AsIs
def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(numpy.int64, adapt_numpy_int64)

engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
connection = engine.connect()
meta = MetaData(bind=engine)

zeta_wallet = Table('zeta_wallet', meta,
                    Column('wallet_id', INTEGER, Sequence('wallet_id_seq'), primary_key=True),
                    Column('DDate', TEXT, nullable=True),
                    Column('From_to_party', TEXT, nullable=True),
                    Column('Credit', TEXT, nullable=True),
                    Column('Debit', TEXT, nullable=True),
                    Column('Balance', TEXT, nullable=True),
                    Column('Bank_ref_txn_id', TEXT, nullable=True),
                    Column('Load', TEXT, nullable=True),
                    Column('Key', TEXT, nullable=True),
                    Column('Lender', TEXT, nullable=True),
                    Column('Zeta_Order_Id', TEXT, nullable=True),
                    Column('DMI_User_bal', TEXT, nullable=True),
                    Column('RC_User_bal', TEXT, nullable=True),
                    Column('DMI_usr_bal_post_may', TEXT, nullable=True),
                    Column('Chandan_comment', TEXT, nullable=True),
                    Column('Current_date', TEXT, nullable=True),
                    Column('Status', TEXT, nullable=True),
                    Column('Unnamed_col1', TEXT, nullable=True),
                    Column('Unnamed_col2', TEXT, nullable=True),
                    Column('Unnamed_col3', TEXT, nullable=True),
                    Column('Unnamed_col4', TEXT, nullable=True),
                    Column('Unnamed_col5', TEXT, nullable=True),
                    Column('Unnamed_col6', TEXT, nullable=True),
                    Column('Unnamed_col7', TEXT, nullable=True),
                    Column('Unnamed_col8', TEXT, nullable=True),
                    Column('Unnamed_col9', TEXT, nullable=True),
                    Column('Actual_bal', TEXT, nullable=True),
                    Column('Data_status',TEXT, nullable=True))

engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
connection = engine.connect()

df2 = pd.read_csv('/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Loads_Zeta_Loads_mod.csv')

if engine.has_table('zeta_wallet') is False:
    zeta_wallet.create()
    ins = zeta_wallet.insert()
    for tup in df2.itertuples():
        engine.execute(zeta_wallet.insert(), DDate=tup[1], From_to_party=tup[2], Credit=tup[3], Debit=tup[4],
                       Balance=tup[5], Bank_ref_txn_id=tup[6], Load=tup[7], Key=tup[8], Lender=tup[9],
                       Zeta_Order_Id=tup[10], DMI_User_bal=tup[11], RC_User_bal=tup[12], DMI_usr_bal_post_may=tup[13],
                       Chandan_comment=tup[14], Current_date=tup[15], Status=tup[16], Unnamed_col1=tup[17],
                       Unnamed_col2=tup[18], Unnamed_col3=tup[19], Unnamed_col4=tup[20], Unnamed_col5=tup[21],
                       Unnamed_col6=tup[22], Unnamed_col7=tup[23], Unnamed_col8=tup[24], Unnamed_col9=tup[25],
                       Actual_bal=tup[26],Data_status = 'NEW')

else:
    df = pd.read_sql_table('zeta_wallet', engine)
    df1 = df.drop(['wallet_id', 'Data_status'], axis=1)
    df1.to_csv('/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Table_data.csv', index=False)
    df1 = pd.read_csv('/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Table_data.csv', index_col=None)
    df1.fillna("None", inplace=True)
    df2.fillna("None", inplace=True)
    flag = 0
    lent = len(df1.columns)


    for j, row in enumerate(df1.itertuples()):
        for k, cow in enumerate(df2.itertuples()):
            flag = 0
            for i in range(1, lent):
                if row[i] != cow[i]:
                    flag = 1
                    if isinstance(row[i], float) and isinstance(cow[i], float) is True:
                        if abs(row[i] - cow[i]) < 1e-2:
                            flag = 0
                        else:
                            break
                    else:
                        break;
            if flag == 0:
                df.set_value(j, 'Data_status', 'DELETED')
                break


    val = df.loc[df['Data_status'] == 'DELETED', 'wallet_id'].values
    tmp = df.loc[df['Data_status'] == 'NEW', 'wallet_id'].values
    stmt = zeta_wallet.update().where(zeta_wallet.c.wallet_id == bindparam('wlt_id')).values(Data_status='DELETED')
    st = []
    for i in val:
        st.append({'wlt_id': i})
    connection.execute(stmt, st)
    ins = zeta_wallet.insert()
    for i, tup in enumerate(df2.itertuples()):
        engine.execute(zeta_wallet.insert(), DDate=tup[1], From_to_party=tup[2], Credit=tup[3], Debit=tup[4],
                   Balance=tup[5], Bank_ref_txn_id=tup[6], Load=tup[7], Key=tup[8], Lender=tup[9],
                   Zeta_Order_Id=tup[10], DMI_User_bal=tup[11], RC_User_bal=tup[12], DMI_usr_bal_post_may=tup[13],
                   Chandan_comment=tup[14], Current_date=tup[15], Status=tup[16], Unnamed_col1=tup[17],
                   Unnamed_col2=tup[18], Unnamed_col3=tup[19], Unnamed_col4=tup[20], Unnamed_col5=tup[21],
                   Unnamed_col6=tup[22], Unnamed_col7=tup[23], Unnamed_col8=tup[24], Unnamed_col9=tup[25],
                   Actual_bal=tup[26], Data_status='NEW')

connection.close()