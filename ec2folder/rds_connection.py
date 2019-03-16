import rds_config
from email_send import email_send

#import pymysql
import psycopg2


#******************* AWS RDS CONFIGURATION ***********************************************
#********************************************************************************************************

host=rds_config.db_host
dbName=rds_config.db_name
uname=rds_config.db_username
upwd=rds_config.db_password
mail_user=rds_config.mail_user
mail_pwd=rds_config.mail_pwd

#********************** AWS RDS Connection Establisation ****************************************
try:
    #charset='utf8'
    conn = psycopg2.connect(host=host,database=dbName, user=uname, password=upwd,charset='utf8')
    
    #conn = pymysql.connect(host=host,database=dbName, user=uname, password=upwd,charset='utf8')
    print('\n********************** AWS RDS Connection Established  ****************************************\n')
except Exception as e:
    email_send('AWS RDS Connection Failed \n RDS_CONFIGURATION ')
    print(e)
