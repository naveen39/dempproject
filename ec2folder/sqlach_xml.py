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
########################################################################

def xml():
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
        Acc_not_ready_to_sync_qry=db.select([accs]).where(accs.columns.ready_to_sync == False).order_by(db.asc(accs.columns.index),)
        AcquerysettoXml = _connection.execute(Acc_not_ready_to_sync_qry).fetchall()
        print('  AcquerysettoXml---',type(AcquerysettoXml),' len ' ,len(AcquerysettoXml))
        #print('Acqueryset oth elm---',)
    except Exception as e:
        print('exp block at try of ---Acc_not_ready_to_sync_qry--- ',e)


    print(AcquerysettoXml[:2],'  Acqueryset---after try ',type(AcquerysettoXml),' len ' ,len(AcquerysettoXml))
    
##    AcquerysettoXml=Account.objects.filter(ready_to_sync=False).order_by('index')[:500]
    print(len(AcquerysettoXml))
##    i=0
    for acc in AcquerysettoXml[:10]:
        print(acc.formtype,'------acc.formtype----')
        print(acc.objectid,acc.ein)
        url2="https://s3.amazonaws.com/irs-form-990/{}_public.xml".format(acc.objectid)
        url=str(acc.url)
        formtype=str(acc.formtype)
        formtype='IRS'+formtype
        payload = ""
        headers = {}
        print(url,formtype)
        try:
            #print(i,'---------------')
            #print(datetime.datetime.now())
            response = requests.request("GET", url, data=payload, headers=headers)
            print('done')
            xmldictform = json.loads(json.dumps(xmltodict.parse(response.content)))
            #pprint(xmldictform['Return']['ReturnData'])
            TaxPeriodEndDt=xmldictform['Return']['ReturnHeader']['TaxPeriodEndDt']
            #pprint(xmldictform['Return']['ReturnData']['IRS990']['CYTotalRevenueAmt'])
            ReturnDataDic=xmldictform['Return']['ReturnData']
            #print(type(ReturnDataDic), formtype in ReturnDataDic.keys(),formtype )
            #checkKey(ReturnDataDic, formtype)TotalRevenueAmt TotalExpensesAmt
            TotalEmployeeCnt=None
            CYTotalRevenueAmt=None
            CYTotalExpensesAmt=None
            if formtype == 'IRS990EZ':
                #print(xmldictform['Return']['ReturnData'][formtype]['TotalRevenueAmt'])
                CYTotalRevenueAmt=xmldictform['Return']['ReturnData'][formtype]['TotalRevenueAmt']
                CYTotalExpensesAmt=xmldictform['Return']['ReturnData'][formtype]['TotalExpensesAmt']
                TotalEmployeeCnt=10
            elif formtype == 'IRS990PF':
                #pprint(xmldictform['Return']['ReturnData'][formtype])
                CYTotalRevenueAmt=xmldictform['Return']['ReturnData'][formtype]['AnalysisOfRevenueAndExpenses']['TotalRevAndExpnssAmt']
                CYTotalExpensesAmt=xmldictform['Return']['ReturnData'][formtype]['AnalysisOfRevenueAndExpenses']['TotalRevAndExpnssAmt']
                #TotalEmployeeCnt=xmldictform['Return']['ReturnData'][formtype]['TotalEmployeeCnt']
            elif formtype == 'IRS990':
                #print(xmldictform['Return']['ReturnData'][formtype]['CYTotalRevenueAmt'])
                CYTotalRevenueAmt=xmldictform['Return']['ReturnData'][formtype]['CYTotalRevenueAmt']
                CYTotalExpensesAmt=xmldictform['Return']['ReturnData'][formtype]['CYTotalExpensesAmt']
                TotalEmployeeCnt=xmldictform['Return']['ReturnData'][formtype]['TotalEmployeeCnt']
            else:
                print('continue')
                #email_send('XMl parsing : We are get other formType = '+str(formtype))
                continue
            #pprint(xmldictform['Return']['ReturnHeader']['Filer'])
            print(TaxPeriodEndDt,' TaxPeriodEndDt ',CYTotalRevenueAmt,'CYTotalRevenueAmt',CYTotalExpensesAmt,' CYTotalExpensesAmt  ',
                  TotalEmployeeCnt,'  TotalEmployeeCnt ')
            print(accs,'--coming for update--',acc.ein)
            acc_up_query = db.update(accs).values(annual_revenue= CYTotalRevenueAmt,
                                                  annual_expenses= CYTotalExpensesAmt,
                                                  number_of_employees= TotalEmployeeCnt if TotalEmployeeCnt is not None else None,
                                                  fiscal_year_end_date= TaxPeriodEndDt,
                                                  ready_to_sync=True)
            query = acc_up_query.where(accs.columns.ein == acc.ein)
            print(query)

            try:
                if(_connection.closed):
                    print('\n----Connection was closed So calling connection method/function---------')
                    db_connection()
                results = _connection.execute(query)
                print('update results------',results)
            except Exception as e:
                print("\n\nAccountrec.save() Exception-- for ****",str(data[processing_year_res][i]['EIN'])+' '+e)
                            

##            acc.annual_revenue= CYTotalRevenueAmt
##            acc.annual_expenses= CYTotalExpensesAmt
##            if TotalEmployeeCnt is not None:
##                acc.number_of_employees= TotalEmployeeCnt
##            acc.fiscal_year_end_date= TaxPeriodEndDt
##            acc.ready_to_sync=True
##            try:
##                acc.save()
##                i=i+1
##            except Exception as e:
##                i=i+1
        except Exception as e:
            
            print(acc.ein,' Exception--****  xml  ',e)
##            email_send(' Exception--****  xml for   '+str(e))
##            #return HttpResponse('Exception--****'+str(e))
##    return HttpResponse('done-- xml processed  '+str(i))


xml()
