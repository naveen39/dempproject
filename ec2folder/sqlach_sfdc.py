import configparser

import sqlalchemy as db
import requests
import datetime

# for xml parsing
import  xmltodict
import json
from pprint import pprint

engine=''
def db_connection():
    global engine
    print("\n----entred in connection---------------")
    engine = db.create_engine('postgresql://irsadmin:admin369@irsinstance.czuzdoyxg3ji.us-east-1.rds.amazonaws.com:5432/ebdb')
    print(engine)
    _connection = engine.connect()
    return _connection

def tosfdc():
    config = configparser.ConfigParser()
    config.read('Refresh_Token.ini')
    access_token=config['Refresh_Token']['access_token']
    instance_url=config['Refresh_Token']['instance_url']
    print('access_token---',config['Refresh_Token']['access_token'])
    """data=re_access(request)
    print(data)"""
    data=''
    if((access_token is not None) and (instance_url is not None)):
        data=sfdcProcessor(access_token,instance_url)
        return HttpResponse(str(data))
    else:
        data=re_access(request)
        

    print(json.dumps(data))
    #return HttpResponse(json.dumps(data))


            

def sfdcProcessor(accessToken, instanceUrl):
    sobject="IRS_Accounts__c"
    externalField="EIN__c"
    instance_url=instanceUrl
    access_token=accessToken
    print('instance_url--',instance_url ,' access_token ',access_token)
    compositeurl=instance_url+"/services/data/v44.0/composite"
    headers={"Content-Type": "application/json; charset=utf-8",
                  "Accept": "application/json",
                  'Authorization': "Bearer "+str(access_token)}

    print('\n entered in irsto rds')
    global engine
    _connection=db_connection()
    print(_connection.closed)
    #print(_connection.close())
    #print(_connection.closed)
    metadata = db.MetaData()

    #-----------------------------------Table metadata------------------

    accs = db.Table('irsTOsfdcIntegration_account', metadata, autoload=True, autoload_with=engine)
    yearTable = db.Table('irsTOsfdcIntegration_year', metadata, autoload=True, autoload_with=engine)
    error_log = db.Table('irsTOsfdcIntegration_error_log', metadata, autoload=True, autoload_with=engine)
    
    print('irsTOsfdcIntegration_account--table',accs)
    #query = db.select([accs])

    #---------------------------------- Querying Records (Accounts) which are ready_to_sync=False---------------

    #if url=='' or url is None:
    try:
        if(_connection.closed):
            print('\n----Connection was closed So calling connection method/function---------')
            db_connection()
        #------------------------- checking record exised by EIN-------------
        Acc_not_synced_to_sfdc_qry=db.select([accs]).where(db.and_(accs.columns.ready_to_sync == True,accs.columns.synced_to_sfdc == False)).order_by(db.asc(accs.columns.index),)
        AcquerysettoSFDC = _connection.execute(Acc_not_synced_to_sfdc_qry).fetchall()
        print('  AcquerysettoXml---',type(AcquerysettoSFDC),' len ' ,len(AcquerysettoSFDC))
        #print('Acqueryset oth elm---',)
    except Exception as e:
        print('exp block at try of ---Acc_not_ready_to_sync_qry--- ',e)


    print(AcquerysettoSFDC[:2],'  AcquerysettoSFDC---after try ',type(AcquerysettoSFDC),' len ' ,len(AcquerysettoSFDC))
    
    #AcquerysettoXml=Account.objects.filter(synced_to_sfdc=False,ready_to_sync=True).order_by('index')[:1]
    print(len(AcquerysettoSFDC))
    if  len(AcquerysettoSFDC) ==0:
        return "No Records to Process"
    
    body={}
    
    listofIRSAccounts=[]
        
    for acc in AcquerysettoSFDC[:24]:
        IRSAccount={}
        fieldmap={}

        fieldmap['Name']=acc.organization
        if acc.objectid is not None:
            fieldmap['IRS_Org_URL__c']=str(acc.url)
        if acc.annual_revenue is not None:
            fieldmap['Annual_Revenue__c']=str(acc.annual_revenue)
        if acc.annual_expenses is not None:
            fieldmap['Annual_Expenses__c']=str(acc.annual_expenses)
        if acc.number_of_employees is not None:
            fieldmap['Number_of_employees__c']=str(acc.number_of_employees)
        if acc.fiscal_year_end_date is not None:
            fieldmap['Fiscal_Year_End_FY__c']=str(acc.fiscal_year_end_date)
        #fieldmap['EIN__c']=str(acc.ein)
        IRSAccount['method']="PATCH"
        IRSAccount['referenceId']=str(acc.ein)
        IRSAccount['url']="/services/data/v44.0/sobjects/{}/{}/{}".format(str(sobject),str(externalField),str(acc.ein))
        IRSAccount['body']=fieldmap
        listofIRSAccounts.append(IRSAccount)
        
        #pprint(IRSAccount)

    body['compositeRequest']=listofIRSAccounts

    pprint(body)
    payload=json.dumps(body)
    response = requests.request("POST", compositeurl, data=payload, headers=headers)
    print(response)
    print(type(response.content),'response----------------',response.content)
    #print('\nres-->',response.text)
    txt = response.text
    data = json.loads(txt)
    ##### the end of the relevant code
    print(type(data),type(data) is dict)
##    if(type(obj) is dict):
##        if "compositeResponse" in obj:
##            print(obj['compositeResponse'])
##            for recRes in obj['compositeResponse']:
##                print('\nrecRes---',recRes)
##                accUpsfdcsync = accs.objects.get(ein=recRes['referenceId'])
##                if 'httpStatusCode' in recRes:
##                    if str(recRes['httpStatusCode'])=='400':
##                        if(('body' in recRes) and (recRes['body'] is not None)):
##                            if 'message' in (recRes['body']):                    
##                                err_msg=recRes['body']['message']
##                                print('err_msg')
##                        else:
##                            print(str(recRes))
##                    else:
##                        print(str(recRes))
##                else:
##                    print(str(recRes))
##        else:
##            print(obj)
##    else:
##        print('response is not entered into dict')
                        
                
            
        
    #data=response.json()
    print('\ndata--',data)
    if "compositeResponse" in data:
        print(data['compositeResponse'])
        for recRes in data['compositeResponse']:
            print('\nrecRes---',recRes)
            
            #accUpsfdcsync = accs.objects.get(ein=recRes['referenceId'])
            if 'httpStatusCode' in recRes:
                acc_up_query=db.update(accs).values()
                if str(recRes['httpStatusCode'])=='204':
                    acc_up_query = db.update(accs).values(synced_to_sfdc=True,sfdc_status='Updated')
##                    accUpsfdcsync.synced_to_sfdc=True
##                    accUpsfdcsync.sfdc_status='Updated'
                if str(recRes['httpStatusCode'])=='201':
                    if(('body' in recRes) and (recRes['body'] is not None)):
                        if 'id' in (recRes['body']):
                            acc_up_query = db.update(accs).values(synced_to_sfdc=True,sfdc_status='Created',sfdc_id=recRes['body']['id'])
##                            accUpsfdcsync.synced_to_sfdc=True
##                            accUpsfdcsync.sfdc_status='Created'                    
##                            accUpsfdcsync.sfdc_id=recRes['body']['id']
                        if 'message' in (recRes['body']):
                            acc_up_query = db.update(accs).values(sfdc_err_msg=recRes['body']['message'])
                            accUpsfdcsync.sfdc_err_msg=recRes['body']['message']
                            
            query = acc_up_query.where(accs.columns.ein == recRes['referenceId'])
            print(query)

            try:
                if(_connection.closed):
                    print('\n----Connection was closed So calling connection method/function---------')
                    db_connection()
                results = _connection.execute(query)
                print('update sfdc results------',results)
            except Exception as e:
                print("\n\nupdate sfdc results Exception-- for ****",str(recRes['referenceId'])+' '+e)
            #accUpsfdcsync.save()

                    


            

            
        pprint(data['compositeResponse'])
        #return data['compositeResponse']
    elif((type(data) is list) and ("errorCode" in data[0])):
        print(data[0]['errorCode'])
        if(data[0]['errorCode']=='INVALID_SESSION_ID'):
            print('entereddddddd==================')
            data=re_access(request)
            print('\n data ---- from reaccess--',data)
            config = configparser.ConfigParser()
            config.read('sfdcConfig.ini')
            config['sfdc_token']=data
            with open('sfdcConfig.ini', 'w') as configfile:
                config.write(configfile)
            sfdcProcessor(request,config['sfdc_token']['access_token'], config['sfdc_token']['instance_url'])

    #elif()        
    
    else:
        print('final els')
        return "final els"



def callback():
    config = configparser.ConfigParser()
    print('entered into callback function --------------------')
    if request.method == 'GET':
        print( 'request.GET.get--------',request.GET.get('code',default=None))
        code=request.GET.get('code',default=None)
        #return HttpResponse(str(request.GET.get('code',default=None)))
    elif request.method == 'POST':
        print( 'request.POST.get--------',request.POST.get('code',default=None))
        code=request.POST.get('code',default=None)
        #return HttpResponse(request.POST.get('code',default=None))
    url = _SFDC_DOMAIN+"/services/oauth2/token?"
    querystring = {"code":code,"redirect_uri":_REDIRECT_URL,
                 "client_id":_CLIENT_ID,"client_secret":_CLIENT_SECRET,"grant_type":"authorization_code"}
    payload = ""
    headers = {    }
    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    print(response.text)
    data=response.json()
    config['Refresh_Token']=data
    #config['zoho_token']['expires_at']=str(datetime.datetime.now() +datetime.timedelta(seconds=3600))
    #config.read('example.ini')
    #print(config['zoho_token']['expires_at'],'---',config['zoho_token']['access_token'])
    with open('Refresh_Token.ini', 'w') as configfile:
        config.write(configfile)
    SfdcConfigins=SfdcConfig(instance_url=data['instance_url'],
        access_token=data['access_token'],
        refresh_token=data['refresh_token'])
    SfdcConfigins.save()
    return HttpResponse(response.text)


def re_access():
    print('entered to re generate acces token')
    config = configparser.ConfigParser()
    configToken = configparser.ConfigParser()
    configTokenUp = configparser.ConfigParser()
    format="%Y-%m-%d %H:%M:%S.%f"
    config.read('Refresh_Token.ini')
    configToken.read('sfdcConfig.ini')
    try:
        print('try  block entered to re generate acces token')
        print('refresh_token---',config['Refresh_Token']['refresh_token'])
        refresh_token=config['Refresh_Token']['refresh_token']
        url = _SFDC_DOMAIN+"/services/oauth2/token"
        print(url,'   ',refresh_token)
        querystring = {"refresh_token":refresh_token,
                           "client_id":_CLIENT_ID,
                           "client_secret":_CLIENT_SECRET,
                           "grant_type":"refresh_token"}
        payload = ""
        headers = {    }
        response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
        
        print(response,'--------- resp refresh')
        """django_response = HttpResponse(
            content=response.content,
                status=response.status_code,
                    content_type=response.headers['Content-Type']
                    )"""
        data=response.json()
        print(data['access_token'])
        if 'error' in data:
            print('error--',data)
            email_send("Dear Admin  \n\n\tSync failed at generating access token \n\n"+str(response.text)+"\
                    \nRefresh Token invliad Please login into Salesforce and Accept access by following link : \
                    \n<u>"+_SFDC_DOMAIN+"/services/oauth2/authorize?response_type=code&client_id="+str(_CLIENT_ID)+"&redirect_uri="+str(_REDIRECT_URL)+"&prompt=consent")
        else:
            print('no errror--')
            access_token=data['access_token']

            print(access_token)
            #configTokenUp['sfdcConfig']=data
            #configTokenUp['zoho_token']['expires_at']=str(datetime.datetime.now() +datetime.timedelta(seconds=3600))
            #configToken.read('sfdcConfig.ini')
            #print(configTokenUp['zoho_token']['expires_at'],'---',configTokenUp['zoho_token']['access_token'])
            with open('sfdcConfig.ini', 'w') as configfile:
                configTokenUp.write(configfile)
            print('data---',data)
            return data
        #return HttpResponse(response.text)
        
    except Exception as e:
        print('Exception  block entered to re generate acces token')
        print('Exception ******** ',e)
        email_send("Dear Admin  \n\n\tSync failed at generating access token \n\n"+str(e)+"\
                    \nRefresh Token invliad Please login into Salesforce and Accept access by following link : \
                    \n<u>"+_SFDC_DOMAIN+"/services/oauth2/authorize?response_type=code&client_id="+str(_CLIENT_ID)+"&redirect_uri="+str(_REDIRECT_URL)+"&prompt=consent")          

    #return configToken['zoho_token']['access_token']


tosfdc()
