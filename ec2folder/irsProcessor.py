import requests,psycopg2

import json
from json import dumps
import os, datetime, time


import rds_config
#from rds_connection import conn
host=rds_config.db_host
dbName=rds_config.db_name
uname=rds_config.db_username
upwd=rds_config.db_password
mail_user=rds_config.mail_user
mail_pwd=rds_config.mail_pwd


import os, sys,errno
from email_send import email_send
from error_handling import error_handling

mydir_name=os.path.join(os.getcwd()+'/logs/'+datetime.datetime.now().strftime('%Y-%m-%d'),'Contact')
print(mydir_name)
try:
    os.makedirs(mydir_name)
    print('mydir_name', str(datetime.datetime.now().strftime('%Y-%m-%d')))
except OSError as e:
    if e.errno != errno.EEXIST:
        raise   #This was not a "directory exist" error..'
    

filename='contact '+str(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))+'.txt'
#filename="COntactest.txt"
module='Contacts'
table_name='staging_contacts_table'

ratetime=0

#________________________________________________________________

#                       RDS COnnection
#_________________________________________________________________

def rdsconnection():
    print('\n----------Entered into RDS connection---------')
    try:
        #charset='utf8'
        conn = psycopg2.connect(host=host,database=dbName, user=uname, password=upwd)
        return conn
        #conn = pymysql.connect(host=host,database=dbName, user=uname, password=upwd,charset='utf8')
        print('\n********************** AWS RDS Connection Established  ****************************************\n')
    except Exception as e:
        #email_send('AWS RDS Connection Failed \n RDS_CONFIGURATION ')
        print(e)
        return str(e)


#________________________________________________________________

#                       IRS 990 Data Procesing
#_________________________________________________________________


def ird900dataprocessor(processing_year):
    print('\-------------Entred into  irs990 proceesor----------------')
    conn=rdsconnection()
    print('conn--',conn)
    #conn.close()
    print('connection is closed-------',conn.closed)
    url="https://s3.amazonaws.com/irs-form-990/index_"+processing_year+".json"
    payload = ""
    headers= {'Content-Type': 'application/json', 'charset': 'utf-8'}
    last_acc_index=None
    try:
        
        print(datetime.datetime.now(),' ',url)
        year_rec=None
        accountIndex_cur = conn.cursor()
        last_index_qty='select ein from "irsTOsfdcIntegration_account"'
        # ORDER BY index  DESC limit 1"
        #SELECT  tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
                        #"
        try:
            accountIndex_cur.execute(last_index_qty)

        except Exception as e:
            print(e)
            raise e
        else:
            pass
        
        for r, field in enumerate(accountIndex_cur):
            def f(field):
                if field is not None:
                    return field
                else :
                    return ''

            last_acc_index=str(f(field[0]))
            print('last_acc_index--',last_acc_index)
        #pass
    except Exception as e:
        raise e
    else:
        pass



ird900dataprocessor('2018')




