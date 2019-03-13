from flask import Flask,request
import requests

#from models import db

from flask_sqlalchemy import SQLAlchemy

from models import *
import datetime

app = application = Flask(__name__)



app.config['DEBUG'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://irsadmin:admin369@irsinstance.czuzdoyxg3ji.us-east-1.rds.amazonaws.com:5432/ebdb'
#db.init_app(app)

db = SQLAlchemy(app)
db.Model.metadata.reflect(db.engine)

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

@app.route("/")
def main():
    return 'Hello World !'



def insertrec():
    #-- object init
    accrec=irsTOsfdcIntegration_account(index =1 ,Year =1 ,
                                        ein =123456789988 ,objectid =123456789988 ,
                                        formtype =' formtye' ,url =' url' ,
                                        organization =' orgnization' 
                                        
                                        )
    #-- field map

    db.session.add(accrec)
    #-- comit
    db.session.commit()
    
@app.route('/tords', methods=['GET', 'POST'])
def tords():
    try:
        print('entered to rds : ')
        insertrec()
        query_results = db.session.query(irsTOsfdcIntegration_account).all()
        #db.session.commit()
        print(query_results[0], type(query_results),len(query_results))
        for rec in query_results:
            print(rec.ein)

        print(query_results[0], type(query_results))
        
        return str(query_results)
        return ('entered to rds : ')
    except Exception as e:
        print('error Exception',str(e))


@app.route('/tords2', methods=['GET', 'POST'])
def tords2():
    print('entered to rds : ')
    acc_last_index_qry=(db.session.query(irsTOsfdcIntegration_account)).order_by(irsTOsfdcIntegration_account.index)
    print(acc_last_index_qry[-1],acc_last_index_qry[-1].index,type(acc_last_index_qry))
    #return str(acc_last_index_qry)
    

            
    init_range=0
    init_range=acc_last_index_qry[-1].index-1
##    if(len(acc_last_index_qry)>0):
##        print('ResultSet--',acc_last_index_qry[-1].index,type(acc_last_index_qry[0][0]))
##        init_range=acc_last_index_qry[-1].index-1
##    else:
##        init_range=0

    processing_year='2018'
    

    
    print(type(init_range),' type  init_range ', init_range)
    #init_range=0
    
    url="https://s3.amazonaws.com/irs-form-990/index_"+processing_year+".json"
    payload = ""
    headers={}
    try:
        print(datetime.datetime.now(),' ',url)
        try:
            year_existed_or_not_qry=db.session.query(irsTOsfdcIntegration_year).filter(irsTOsfdcIntegration_year.year_integer == processing_year)
            print('year_existed_or_not_qry--------------------',year_existed_or_not_qry)

            #year = ResultProxy.fetchone()
            #print(year is None,'---year--',year)
            if(year_existed_or_not_qry is None):
                try:
                    year = irsTOsfdcIntegration_year(
                            year_integer=processing_year,
                                updated_date=datetime.datetime.now(),
                                created_date=datetime.datetime.now())
                    #-- field map
                    db.session.add(year)
                    #-- comit
                    db.session.commit()
                except Exception as e:
                    print('Exception as e--',e)
                    raise e
                else:
                    print('-inserted-')
    
                
            else:
                print(year_existed_or_not_qry,type(year_existed_or_not_qry))
                for i in year_existed_or_not_qry:
                    print(i.id,'------i--',i)

##                #year=Year.objects.filter(year_integer=processing_year)
##                #print('year--',year, len(year))
        except Exception as e:
            print(' year insertion failed erorr at db operations calling ', e)

##        response = requests.request("GET", url, data=payload, headers=headers)
##        print('response---',response)
##        print('done')
##        data=response.json()
##        print('--after json convertion ')
##        django_response = HttpResponse(
##            content=response.content,
##                status=response.status_code,
##                    content_type=response.headers['Content-Type']
##                    )
##        processing_year_res="Filings"+processing_year
##        print(len(data[processing_year_res]))
##        Total_Records_res=len(data[processing_year_res])
##        print(datetime.datetime.now())
##        processed_upto=0
##        Success_Records=0
##        
##
##        
##            #year.save()
##
##
##        
##        #init_range=0
##        max_range=10
##        #
##        for i in range(init_range,init_range+max_range):
##            print('\n next element')
##            print(_connection)
##            print(str(data[processing_year_res][i]['EIN']))
##            try:
##                #processed_upto+=1
##                print('entered into for try block \n',_connection)
##
##                try:
##                    if(_connection.closed):
##                        print('\n----Connection was closed So calling connection method/function---------')
##                        db_connection()
##                    #------------------------- checking record exised by EIN-------------
##
##                    Acc_ein_qry=db.select([accs.columns.ein]).where(accs.columns.ein == str(data[processing_year_res][i]['EIN']))
##                    Acqueryset = _connection.execute(Acc_ein_qry).fetchall()
##                    print(Acqueryset,'  Acqueryset---',type(Acqueryset),' len ' ,len(Acqueryset))
##                    print('Acqueryset oth elm---',)
##
##                except Exception as e:
##                    print('exp block at try ',e)
##                #Acqueryset=Account.objects.filter(ein=str(data[processing_year_res][i]['EIN']))
##                
##                #----------------------------If records matched then cheking its updated or not ----------
##
##                if(len(Acqueryset)>0 and len(Acqueryset)<2):
##                    print(type(Acqueryset[0]),'  Existed record  ',len(Acqueryset) )
##                    if(_connection.closed):
##                        print('\n----Connection was closed So calling connection method/function---------')
##                        db_connection()
##
##                    Acc_ein_lastupdate_qry=db.select([accs.columns.index]).where(
##                        db.and_(accs.columns.ein == str(data[processing_year_res][i]['EIN']),
##                        accs.columns.lastupdated == str(data[processing_year_res][i]['LastUpdated'])))
##                    print('Acc_ein_lastupdate_qry--',Acc_ein_lastupdate_qry)
##                    AcquerysetbyDate = _connection.execute(Acc_ein_lastupdate_qry).fetchall()
##                    #AcquerysetbyDate=Account.objects.filter(ein=str(data[processing_year_res][i]['EIN']),lastupdated=str(data[processing_year_res][i]['LastUpdated']))
##                    if(len(AcquerysetbyDate)==0):
##                        print('coming for update--',Acqueryset[0].ein)
##                        acc_up_query = db.update(accs).values(ein=Acqueryset[0].ein,
##                            organization=str(data[processing_year_res][i]['OrganizationName']),
##                        url=str(data[processing_year_res][i]['URL']),
##                        objectid=str(data[processing_year_res][i]['ObjectId']),
##                        formtype=str(data[processing_year_res][i]['FormType']),
##                        submittedon=str(data[processing_year_res][i]['SubmittedOn']),
##                        lastupdated=str(data[processing_year_res][i]['LastUpdated']),
##                        Year_id=year.id,
##                        index=str(i),
##                        ready_to_sync=False)
##                        query = acc_up_query.where(accs.columns.ein == Acqueryset[0].ein)
##                        
##                        
##                        try:
##                            if(_connection.closed):
##                                print('\n----Connection was closed So calling connection method/function---------')
##                                db_connection()
##
##                            results = _connection.execute(query)
##                            
##                        except Exception as e:
##                            print("\n\nAccountrec.save() Exception-- for ****",str(data[processing_year_res][i]['EIN'])+' '+e)
##                            
##                            error_log_insert_query = db.insert(error_log).values(
##                                    EIN_F=str(data[processing_year_res][i]['EIN']),
##                                    Error_Message=str(e),
##                                    index=str(i),
##                                    Year_id=year.id,
##                                    updated_date=datetime.datetime.now()
##                                    )
##                            errResultProxy = _connection.execute(error_log_insert_query)
##                            #Error_Logs.save()
##                            print(errResultProxy)
##                        else:
##                            print('\n ------------- Updated ------------------\n')
##
##                        
##                    else:
##                        print('******** No changed *****')
##                        isChanged=False
##                        continue
##
##                
##                elif(len(Acqueryset)>1):
##                    print('Dulpicates existed')
##                    
##                else:
##                    print('Not  existed--')
##                    print(Acqueryset,'---Acqueryset--',year.id,' type of ac id--', type(year))
##                    #continue
##                    acc_insert_query = db.insert(accs).values(ein=str(data[processing_year_res][i]['EIN']),
##                            organization=str(data[processing_year_res][i]['OrganizationName']),
##                        url=str(data[processing_year_res][i]['URL']),
##                        objectid=str(data[processing_year_res][i]['ObjectId']),
##                        formtype=str(data[processing_year_res][i]['FormType']),
##                        submittedon=str(data[processing_year_res][i]['SubmittedOn']),
##                        lastupdated=str(data[processing_year_res][i]['LastUpdated']),
##                        Year_id=year.id,
##                        index=str(i),
##                        ready_to_sync=False,
##                        synced_to_sfdc=False,
##                        created_date=datetime.datetime.now(),
##                        updated_date=datetime.datetime.now())
##                    print('acc_insert_query--',acc_insert_query)
##                    
##                    try:
##                        ResultProxy = _connection.execute(acc_insert_query)
##
##
##                    except Exception as e:
##                        print('Accountrec.save() Exception--  ****',str(data[processing_year_res][i]['EIN'])+' ',e)
##                        error_log_insert_query = db.insert(error_log).values(
##                                    EIN_F=str(data[processing_year_res][i]['EIN']),
##                                    Error_Message=str(e),
##                                    index=str(i),
##                                    Year_id=year.id,
##                                    updated_date=datetime.datetime.now()
##                                    )
##                        errResultProxy = _connection.execute(error_log_insert_query)
##                            #Error_Logs.save()
##                        print(errResultProxy)
##                    else:
##                        print('\n ------------- inserted ------------------\n')
##                
##            except Exception as e:
##                print('Exception--**** irsToDB single rec ',e)
##                email_send('Exception--**** irsToDB single rec '+str(data[processing_year_res][i]['EIN'])+' '+str(e))
##                error_log_insert_query = db.insert(error_log).values(
##                                    EIN_F=str(data[processing_year_res][i]['EIN']),
##                                    Error_Message=str(e),
##                                    index=str(i),
##                                    Year_id=year.id,
##                                    updated_date=datetime.datetime.now()
##                                    )
##                errResultProxy = _connection.execute(error_log_insert_query)
##                            #Error_Logs.save()
##                print(errResultProxy)
##        try:        
##            print(' going to update year',Total_Records_res)    
##            yr_update =db.update(yearTable).values(Total_Records=Total_Records_res)
##            yr_update_qry=yr_update.where(yearTable.columns.id==year.id)
##            print('yr_update_qry--',yr_update_qry)
##        #year[0].Total_Records=Total_Records_res
##        #year[0].Success_Records=year[0].Success_Records+Success_Records
##        
##            results = _connection.execute(yr_update_qry)
##            print(results)
##        except Exception as e:
##            print(e)
##            raise e
##        else:
##            print('---account updation done ')
##        #results = _connection.execute(yr_update_qry)
##        #year[0].save()
##        
##        #Year.objects.filter(year_integer=2017).update(processed=processed_upto)
##        return HttpResponse("success")
##        #return django_response
##            
##            
    except Exception as e:
        print('Exception--**** irsToDB  ',e)
##        email_send('Exception--**** irsToDB  '+str(e))
##        return django_response
##        return HttpResponse('Exception--**** irsToDB '+str(e))
##
##    return django_response

##    if(len(ResultSet)>0):
##        print('ResultSet--',ResultSet[0][0],type(ResultSet[0][0]))
##        init_range=ResultSet[0][0]-1
##    else:
##        init_range=0
##
##    processing_year='2018'
##    
##
##    
##    print(type(init_range),' type  init_range ', init_range)
##    #init_range=0
##    
##    url="https://s3.amazonaws.com/irs-form-990/index_"+processing_year+".json"
##    payload = ""
##    headers={}
##    try:
##        print(datetime.datetime.now(),' ',url)
##        try:
##            print(' before year--calling ',_connection)
##            if(_connection.closed):
##                print('\n----Connection was closed So calling connection method/function---------')
##                db_connection()
##            else:
##                year_existed_or_not_qry=db.select([yearTable]).where(yearTable.columns.year_integer == processing_year)
##                ResultProxy = _connection.execute(year_existed_or_not_qry)
##                print(ResultProxy)
##
##                year = ResultProxy.fetchone()
##                print(year is None,'---year--',year)
##                if(year is None):
##                    year = db.Table(yearTable, metadata, autoload=True, autoload_with=engine)
##    
##                    try:
##                        query = db.insert(yearTable).values(
##                            year_integer=processing_year,
##                            updated_date=datetime.datetime.now(),
##                            created_date=datetime.datetime.now())
##                        year = _connection.execute(query)
##                        print(year)
##                    except Exception as e:
##                        print('Exception as e--',e)
##                        raise e
##                    else:
##                        pass
##                    
##                    #year=Year(year_integer=processing_year)
##                else:
##                    print(year.id)
##
##                #year=Year.objects.filter(year_integer=processing_year)
##                #print('year--',year, len(year))
##        except:
##            print(' year insertion failed erorr at db operations calling ')
##
##        response = requests.request("GET", url, data=payload, headers=headers)
##        print('response---',response)
##        print('done')
##        data=response.json()
##        print('--after json convertion ')
##        django_response = HttpResponse(
##            content=response.content,
##                status=response.status_code,
##                    content_type=response.headers['Content-Type']
##                    )
##        processing_year_res="Filings"+processing_year
##        print(len(data[processing_year_res]))
##        Total_Records_res=len(data[processing_year_res])
##        print(datetime.datetime.now())
##        processed_upto=0
##        Success_Records=0
##        
##
##        
##            #year.save()
##
##
##        
##        #init_range=0
##        max_range=10
##        #
##        for i in range(init_range,init_range+max_range):
##            print('\n next element')
##            print(_connection)
##            print(str(data[processing_year_res][i]['EIN']))
##            try:
##                #processed_upto+=1
##                print('entered into for try block \n',_connection)
##
##                try:
##                    if(_connection.closed):
##                        print('\n----Connection was closed So calling connection method/function---------')
##                        db_connection()
##                    #------------------------- checking record exised by EIN-------------
##
##                    Acc_ein_qry=db.select([accs.columns.ein]).where(accs.columns.ein == str(data[processing_year_res][i]['EIN']))
##                    Acqueryset = _connection.execute(Acc_ein_qry).fetchall()
##                    print(Acqueryset,'  Acqueryset---',type(Acqueryset),' len ' ,len(Acqueryset))
##                    print('Acqueryset oth elm---',)
##
##                except Exception as e:
##                    print('exp block at try ',e)
##                #Acqueryset=Account.objects.filter(ein=str(data[processing_year_res][i]['EIN']))
##                
##                #----------------------------If records matched then cheking its updated or not ----------
##
##                if(len(Acqueryset)>0 and len(Acqueryset)<2):
##                    print(type(Acqueryset[0]),'  Existed record  ',len(Acqueryset) )
##                    if(_connection.closed):
##                        print('\n----Connection was closed So calling connection method/function---------')
##                        db_connection()
##
##                    Acc_ein_lastupdate_qry=db.select([accs.columns.index]).where(
##                        db.and_(accs.columns.ein == str(data[processing_year_res][i]['EIN']),
##                        accs.columns.lastupdated == str(data[processing_year_res][i]['LastUpdated'])))
##                    print('Acc_ein_lastupdate_qry--',Acc_ein_lastupdate_qry)
##                    AcquerysetbyDate = _connection.execute(Acc_ein_lastupdate_qry).fetchall()
##                    #AcquerysetbyDate=Account.objects.filter(ein=str(data[processing_year_res][i]['EIN']),lastupdated=str(data[processing_year_res][i]['LastUpdated']))
##                    if(len(AcquerysetbyDate)==0):
##                        print('coming for update--',Acqueryset[0].ein)
##                        acc_up_query = db.update(accs).values(ein=Acqueryset[0].ein,
##                            organization=str(data[processing_year_res][i]['OrganizationName']),
##                        url=str(data[processing_year_res][i]['URL']),
##                        objectid=str(data[processing_year_res][i]['ObjectId']),
##                        formtype=str(data[processing_year_res][i]['FormType']),
##                        submittedon=str(data[processing_year_res][i]['SubmittedOn']),
##                        lastupdated=str(data[processing_year_res][i]['LastUpdated']),
##                        Year_id=year.id,
##                        index=str(i),
##                        ready_to_sync=False)
##                        query = acc_up_query.where(accs.columns.ein == Acqueryset[0].ein)
##                        
##                        
##                        try:
##                            if(_connection.closed):
##                                print('\n----Connection was closed So calling connection method/function---------')
##                                db_connection()
##
##                            results = _connection.execute(query)
##                            
##                        except Exception as e:
##                            print("\n\nAccountrec.save() Exception-- for ****",str(data[processing_year_res][i]['EIN'])+' '+e)
##                            
##                            error_log_insert_query = db.insert(error_log).values(
##                                    EIN_F=str(data[processing_year_res][i]['EIN']),
##                                    Error_Message=str(e),
##                                    index=str(i),
##                                    Year_id=year.id,
##                                    updated_date=datetime.datetime.now()
##                                    )
##                            errResultProxy = _connection.execute(error_log_insert_query)
##                            #Error_Logs.save()
##                            print(errResultProxy)
##                        else:
##                            print('\n ------------- Updated ------------------\n')
##
##                        
##                    else:
##                        print('******** No changed *****')
##                        isChanged=False
##                        continue
##
##                
##                elif(len(Acqueryset)>1):
##                    print('Dulpicates existed')
##                    
##                else:
##                    print('Not  existed--')
##                    print(Acqueryset,'---Acqueryset--',year.id,' type of ac id--', type(year))
##                    #continue
##                    acc_insert_query = db.insert(accs).values(ein=str(data[processing_year_res][i]['EIN']),
##                            organization=str(data[processing_year_res][i]['OrganizationName']),
##                        url=str(data[processing_year_res][i]['URL']),
##                        objectid=str(data[processing_year_res][i]['ObjectId']),
##                        formtype=str(data[processing_year_res][i]['FormType']),
##                        submittedon=str(data[processing_year_res][i]['SubmittedOn']),
##                        lastupdated=str(data[processing_year_res][i]['LastUpdated']),
##                        Year_id=year.id,
##                        index=str(i),
##                        ready_to_sync=False,
##                        synced_to_sfdc=False,
##                        created_date=datetime.datetime.now(),
##                        updated_date=datetime.datetime.now())
##                    print('acc_insert_query--',acc_insert_query)
##                    
##                    try:
##                        ResultProxy = _connection.execute(acc_insert_query)
##
##
##                    except Exception as e:
##                        print('Accountrec.save() Exception--  ****',str(data[processing_year_res][i]['EIN'])+' ',e)
##                        error_log_insert_query = db.insert(error_log).values(
##                                    EIN_F=str(data[processing_year_res][i]['EIN']),
##                                    Error_Message=str(e),
##                                    index=str(i),
##                                    Year_id=year.id,
##                                    updated_date=datetime.datetime.now()
##                                    )
##                        errResultProxy = _connection.execute(error_log_insert_query)
##                            #Error_Logs.save()
##                        print(errResultProxy)
##                    else:
##                        print('\n ------------- inserted ------------------\n')
##                
##            except Exception as e:
##                print('Exception--**** irsToDB single rec ',e)
##                email_send('Exception--**** irsToDB single rec '+str(data[processing_year_res][i]['EIN'])+' '+str(e))
##                error_log_insert_query = db.insert(error_log).values(
##                                    EIN_F=str(data[processing_year_res][i]['EIN']),
##                                    Error_Message=str(e),
##                                    index=str(i),
##                                    Year_id=year.id,
##                                    updated_date=datetime.datetime.now()
##                                    )
##                errResultProxy = _connection.execute(error_log_insert_query)
##                            #Error_Logs.save()
##                print(errResultProxy)
##        try:        
##            print(' going to update year',Total_Records_res)    
##            yr_update =db.update(yearTable).values(Total_Records=Total_Records_res)
##            yr_update_qry=yr_update.where(yearTable.columns.id==year.id)
##            print('yr_update_qry--',yr_update_qry)
##        #year[0].Total_Records=Total_Records_res
##        #year[0].Success_Records=year[0].Success_Records+Success_Records
##        
##            results = _connection.execute(yr_update_qry)
##            print(results)
##        except Exception as e:
##            print(e)
##            raise e
##        else:
##            print('---account updation done ')
##        #results = _connection.execute(yr_update_qry)
##        #year[0].save()
##        
##        #Year.objects.filter(year_integer=2017).update(processed=processed_upto)
##        return HttpResponse("success")
##        #return django_response
##            
##            
##    except Exception as e:
##        print('Exception--**** irsToDB  ',e)
##        email_send('Exception--**** irsToDB  '+str(e))
##        return django_response
##        return HttpResponse('Exception--**** irsToDB '+str(e))
##
##    return django_response


if __name__ == '__main__':
    app.run()

