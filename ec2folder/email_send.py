
#**************************************method to send Email **********************************************
#**************************************************************************************************************
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import rds_config

from error_handling import error_handling

mail_user=rds_config.mail_user
mail_pwd=rds_config.mail_pwd

def email_send(program_name):
     fromaddr = mail_user
     #recipients = ['bnaveen@dhruvsoft.com', 'ramanji@dhruvsoft.com','adam.steele@kw.com','maps@kw.com','rudo.chifamba@kw.com']
     toaddr = "bnaveen@dhruvsoft.com"
     msg = MIMEMultipart()
     msg['From'] = fromaddr
     msg['To'] = toaddr
     msg['Subject'] = "Whitepages to Zoho Sync Failure"
     
     body = " "+program_name+".py exception \n  --- "
     body=body+str(error_handling())
     msg.attach(MIMEText(body, 'plain'))
     server = smtplib.SMTP('smtp.gmail.com', 587)
     server.starttls()
     server.login(fromaddr, mail_pwd)
     text = msg.as_string()
     server.sendmail(fromaddr, toaddr , text)
     server.quit()
