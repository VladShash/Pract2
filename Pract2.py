import pandas as pd
df1=pd.read_table('users.tsv')
df1=df1[df1['country_code'].notnull()]
L=list()
for x in df1['app']:
    x=x.lower()
    L.append(x)
df1['app']=pd.Series(L)
print(set(df1['app'].values))
print('Some of the values "mobile","desktop" are case sensitive ("Mobile" and "mobile" are different f.e.)')
df2=pd.read_table('activities.tsv')
print('Some of the values of contact id are not in users.tsv. Perhaps they are the moderators id')
L=list()
for x in df2['user_app']:
    x=x.lower()
    L.append(x)
df2['user_app']=pd.Series(L)
print(set(df2['user_app']))
print('Some of the values "mobile","desktop" are case sensitive ("Mobile" and "mobile" are different f.e.)')

df3=pd.read_table('user_session_end.tsv')
print('Some sessions are over a day long. Deleting those which are over a week long (e.g. 1.4 million seconds)')
df3=df3[df3['duration']<=3600*24*7]
df4=pd.read_table('orders.tsv')
df5=pd.read_table('subchannels_cost.tsv')
df6=pd.read_table('subscription_status.tsv')
print('Some people subscribed more than one time (some even in a short span of time)')
df6=df6.groupby(by=['parent_order_id','status'])
df7=pd.read_table('activity_type.tsv')
df8=pd.read_table('context.tsv')
df9=pd.read_table('service_type.tsv')
import vertica_python as vp
from verticapy.utilities import pandas_to_vertica
conn_cluster={'host':'localhost',
              'port':5433,
              'user':'dbadmin',
              'password':'',
              'database':'VMart'}
with vp.connect(**conn_cluster) as connection:
    cur=connection.cursor()
    pandas_to_vertica(df=df1, name='users', cursor=cur, schema='product')
    pandas_to_vertica(df=df2, name='activities', cursor=cur, schema='product')
    pandas_to_vertica(df=df3, name='user_session_end', cursor=cur, schema='product')
    pandas_to_vertica(df=df4, name='orders', cursor=cur, schema='marketing')
    pandas_to_vertica(df=df5, name='subchannels_cost', cursor=cur, schema='marketing')
    pandas_to_vertica(df=df6, name='subscription_status', cursor=cur, schema='marketing')
    pandas_to_vertica(df=df7, name='activity_type', cursor=cur, schema='dictionary')
    pandas_to_vertica(df=df8, name='context', cursor=cur, schema='dictionary')
    pandas_to_vertica(df=df9, name='service_type', cursor=cur, schema='dictionary')
    connection.commit()
    cur.closed()