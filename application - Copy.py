from flask import Flask,request
import requests

#from models import db

from flask_sqlalchemy import SQLAlchemy

from models import *
import datetime,time
from timeit import default_timer as timer 

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
    print('--------------scheduler started')
    return '--------------scheduler started'
    """try:
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
        print('error Exception',str(e))"""


@app.route('/tords2', methods=['GET', 'POST'])
def tords2():
    start = timer() 
    #start = time.time()
    print('entered to rds : ',start)
    processing_year='2018'
    acc_last_index_qry=(db.session.query(irsTOsfdcIntegration_account)).order_by(irsTOsfdcIntegration_account.index)
    try:
        some_object_iterator = iter(acc_last_index_qry)
        init_range=acc_last_index_qry[-1].index-1
    except TypeError as te:
        init_range=0

    
   
    print(type(init_range),' type  init_range ', init_range)
    #init_range=0
    
    url="https://s3.amazonaws.com/irs-form-990/index_"+processing_year+".json"
    payload = ""
    headers={}
    try:
        print(datetime.datetime.now(),' ',url)
        year_rec=None
        try:
            year_existed_or_not_qry=db.session.query(irsTOsfdcIntegration_year).filter_by(year_integer=processing_year)
            print('year_existed_or_not_qry--------------------',year_existed_or_not_qry)
            if(year_existed_or_not_qry.scalar() is None):
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
                    print('-inserted acc-')
    
                
            else:
                print(' yr existed')
                year_existed_or_not_qry=year_existed_or_not_qry.first()
                print(year_existed_or_not_qry,type(year_existed_or_not_qry))
                #for i in year_existed_or_not_qry:
                print(year_existed_or_not_qry.id,'------i--')
                year_rec=year_existed_or_not_qry
                print(year_rec.id,'-----------yr rec type--',year_rec,)
                # update_yr=db.session.query(irsTOsfdcIntegration_year).filter_by(year_integer=processing_year).one()
                # update_yr.Total_Records=123
                # db.session.commit()
##                
        except Exception as e:
            print(' year insertion failed erorr at db operations calling ', e)

        #-----        testing

        # try:
        #     #------------------------- checking record exised by EIN-------------
        #     Acc_ein_qry=db.session.query(irsTOsfdcIntegration_account).filter_by(ein=453578215).count()
        #     #print(' count----',Acc_ein_qry)
        #     Acc_ein_qry1=db.session.query(irsTOsfdcIntegration_account).filter_by(ein=123456789121)
        #     #print(' -Acc_ein_qry 1 ---',Acc_ein_qry1)
        #     print(' -Acc_ein_qry 1 ---',Acc_ein_qry1.scalar())
        #     if(Acc_ein_qry1.scalar() is None):
        #         try:
        #             acc_insert=irsTOsfdcIntegration_account(
        #                         index=str(2),
        #                         Year_id=year_rec.id,
        #                         ein=12345678912,
        #                         objectid=12345678911,
        #                         formtype=str('FormType'),
        #                         url=str('URL'),
        #                         organization=str('OrganizationName'),
        #                         annual_revenue=None,
        #                         annual_expenses=None,
        #                         number_of_employees=None,
        #                         submittedon='2108-10-10',
        #                         fiscal_year_end_date=None,
        #                         lastupdated='2108-10-10',
        #                         ready_to_sync=False,
        #                         synced_to_sfdc=False,
        #                         sfdc_id=None,
        #                         sfdc_status=None,
        #                         sfdc_err_msg=None,
        #                         created_date=datetime.datetime.now(),
        #                         updated_date=datetime.datetime.now())
                        
        #                 #-- field map
        #             db.session.add(acc_insert)
        #                     #-- comit
        #             db.session.commit()
        #         except Exception as e:
        #             print('-----------Inserted error-----------',e)
        #             db.session.rollback()
        #             try:
        #                 error_log=irsTOsfdcIntegration_error_log(
        #                 EIN_F=str(12345678912),
        #                     Error_Message=str(e),
        #                     index=str(5),
        #                     Year_id=year_rec.id,
        #                     updated_date=datetime.datetime.now())

        #                 #-- field map
        #                 db.session.add(error_log)
        #                 #-- comit
        #                 db.session.commit()
        #             except Exception as e:
        #                 print('-----------Inserted Errorlog error-----------',e)
        #             else:
        #                 pass
        #             raise e
        #         else:
        #             print(' insert else------')
        #             print('-----------Inserted Successfully-----------',acc_insert.ein)
                    

        #     Acc_ein_qry2=db.session.query(irsTOsfdcIntegration_account).filter_by(ein=453578215)
        #     #print(' -Acc_ein_qry exisetd 2 ---',Acc_ein_qry2)
        #     print(' -Acc_ein_qry --- 2',Acc_ein_qry2.scalar())
        #     if(Acc_ein_qry2.scalar() is not None):
        #         if(Acc_ein_qry2.count()==1):
        #             for acc_rec in Acc_ein_qry2:
        #                 acc_rec.index=str(2)
        #                 acc_rec.Year_id=acc_rec.Year_id
        #                 acc_rec.ein=acc_rec.ein
        #                 acc_rec.objectid=acc_rec.objectid
        #                 acc_rec.formtype='upadted formtype'
        #                 acc_rec.url=acc_rec.url
        #                 acc_rec.organization=acc_rec.organization
        #                 acc_rec.annual_revenue=acc_rec.annual_revenue
        #                 acc_rec.annual_expenses=acc_rec.annual_expenses
        #                 acc_rec.number_of_employees=acc_rec.number_of_employees
        #                 acc_rec.submittedon=acc_rec.submittedon
        #                 acc_rec.fiscal_year_end_date=acc_rec.fiscal_year_end_date
        #                 acc_rec.lastupdated=acc_rec.lastupdated
        #                 acc_rec.ready_to_sync=False
        #                 acc_rec.synced_to_sfdc=False
        #                 acc_rec.sfdc_id=acc_rec.sfdc_id
        #                 acc_rec.sfdc_status=acc_rec.sfdc_status
        #                 acc_rec.sfdc_err_msg=acc_rec.sfdc_err_msg
        #                 acc_rec.created_date=datetime.datetime.now()
        #                 acc_rec.updated_date=datetime.datetime.now()
        #                 try:
        #                     db.session.commit()
        #                 except Exception as e:
        #                     print('-----------Updated error-----------',e)
        #                     raise e
        #                 else:
        #                     print('============upadtion done======')

                    

        # except Exception as e:
        #     raise e
        # else:
        #     pass
        #_____________________________________________________________________________
        
        #                          IRS 990 DATA Processing
        #_____________________________________________________________________________
        st = timer() 
        print(st)
        response = requests.request("GET", url, data=payload, headers=headers)
        #print('response---',response)
        ended = timer() 
        print(ended - st,' request processing time')
        print('done')
        data=response.json()
        print('--after json convertion ')
        """django_response = HttpResponse(
            content=response.content,
                status=response.status_code,
                    content_type=response.headers['Content-Type']
                    )"""
        processing_year_res="Filings"+processing_year
        print(len(data[processing_year_res]))
        Total_Records_res=len(data[processing_year_res])
        print(datetime.datetime.now())
        processed_upto=0
        Success_Records=0
        #init_range=0
        max_range=10
        try:
            for i in range(init_range,init_range+max_range):
                print('\n for loop  element----',i)

                try:
                    #------------------------- checking record exised by EIN-------------
                    Acc_ein_qry=db.session.query(irsTOsfdcIntegration_account).filter_by(ein=str(data[processing_year_res][i]['EIN']))
                    print(' count----',Acc_ein_qry.scalar(),Acc_ein_qry.scalar() is None)
                    if(Acc_ein_qry.scalar() is None):

                        #_____________________________________Insert new Account______________

                        try:
                            acc_insert=irsTOsfdcIntegration_account(
                                index=str(i),
                                Year_id=year_rec.id,
                                ein=str(data[processing_year_res][i]['EIN']),
                                objectid=str(data[processing_year_res][i]['ObjectId']),
                                formtype=str(data[processing_year_res][i]['FormType']),
                                url=str(data[processing_year_res][i]['URL']),
                                organization=str(data[processing_year_res][i]['OrganizationName']),
                                annual_revenue=None,
                                annual_expenses=None,
                                number_of_employees=None,
                                submittedon=str(data[processing_year_res][i]['SubmittedOn']),
                                fiscal_year_end_date=None,
                                lastupdated=str(data[processing_year_res][i]['LastUpdated']),
                                ready_to_sync=False,
                                synced_to_sfdc=False,
                                sfdc_id=None,
                                sfdc_status=None,
                                sfdc_err_msg=None,
                                created_date=datetime.datetime.now(),
                                updated_date=datetime.datetime.now())
                        
                            #-- field map
                            db.session.add(acc_insert)
                            #-- comit
                            db.session.commit()
                        except Exception as e:
                            print('-----------Inserted Error-----------',e)
                            try:
                                error_log=irsTOsfdcIntegration_error_log(
                                EIN_F=str(data[processing_year_res][i]['EIN']),
                                    Error_Message=str(e),
                                    index=str(i),
                                    Year_id=year_rec.id,
                                    updated_date=datetime.datetime.now())

                                #-- field map
                                db.session.add(error_log)
                                #-- comit
                                db.session.commit()
                            except Exception as e:
                                print('-----------Inserted Errorlog error-----------',e)
                            else:
                                pass
                            
                            raise e
                        else:
                            print('-----------Inserted Successfully-----------',acc_insert.ein)
                    
                    else:
                        print(Acc_ein_qry.count(),'-------No of recs matched with ein')
                        if(Acc_ein_qry.count()==1):
                            Acc_ein_update_qry=db.session.query(
                                                    irsTOsfdcIntegration_account).filter_by(
                                                            ein=str(data[processing_year_res][i]['EIN']),
                                                            lastupdated = str(data[processing_year_res][i]['LastUpdated']))
                            print(' count----',Acc_ein_update_qry.scalar(),Acc_ein_update_qry.scalar() is None)
                            if(Acc_ein_update_qry.scalar() is None):
                                try:
                                    print('------Record Updated ------------------')
                                    if(Acc_ein_update_qry.count()==1):
                                        for acc_rec in Acc_ein_update_qry:
                                            acc_rec.index=acc_rec.index
                                            acc_rec.Year_id=acc_rec.Year_id
                                            acc_rec.ein=str(data[processing_year_res][i]['EIN'])
                                            acc_rec.objectid=str(data[processing_year_res][i]['ObjectId'])
                                            acc_rec.formtype=acc_rec.formtype
                                            acc_rec.url=str(data[processing_year_res][i]['URL'])
                                            acc_rec.organization=str(data[processing_year_res][i]['OrganizationName'])
                                            acc_rec.annual_revenue=acc_rec.annual_revenue
                                            acc_rec.annual_expenses=acc_rec.annual_expenses
                                            acc_rec.number_of_employees=acc_rec.number_of_employees
                                            acc_rec.submittedon=str(data[processing_year_res][i]['SubmittedOn'])
                                            acc_rec.fiscal_year_end_date=acc_rec.fiscal_year_end_date
                                            acc_rec.lastupdated=str(data[processing_year_res][i]['LastUpdated'])
                                            acc_rec.ready_to_sync=False
                                            acc_rec.synced_to_sfdc=False
                                            acc_rec.sfdc_id=acc_rec.sfdc_id
                                            acc_rec.sfdc_status=acc_rec.sfdc_status
                                            acc_rec.sfdc_err_msg=acc_rec.sfdc_err_msg
                                            acc_rec.created_date=datetime.datetime.now()
                                            acc_rec.updated_date=datetime.datetime.now()
                                            try:
                                                db.session.commit()
                                            except Exception as e:
                                                print('============upadtion error======',e)
                                                raise e
                                            else:
                                                print('============upadtion done======')

                                except Exception as e:
                                    raise e
                                else:
                                    pass
                            else:
                                print('------------------Nothing is changed--------------')
                        if(Acc_ein_qry.count()>1):
                            print('------------please check multiple recs existed---------')

                except Exception as e:
                    raise e
                else:
                    pass
            
            print(' going to update year',Total_Records_res)

            update_yr=db.session.query(irsTOsfdcIntegration_year).filter_by(year_integer=processing_year).one()
            update_yr.Total_Records=Total_Records_res
            db.session.commit()    
##            yr_update =db.update(yearTable).values(Total_Records=Total_Records_res)
##            yr_update_qry=yr_update.where(yearTable.columns.id==year.id)
##            print('yr_update_qry--',yr_update_qry)

        except Exception as e:
            raise e
        else:
            pass
        finally:
            pass
##             
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
        end = timer() 
        print(end - start)
        print('Exception--**** irsToDB  ',e)
        return 'Exception--**** irsToDB  '+str(e)
    else:
        end = timer() 
        print(end - start)
        return "done "
##        email_send('Exception--**** irsToDB  '+str(e))
##        return django_response
##        return HttpResponse('Exception--**** irsToDB '+str(e))
##
##    
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
##    


if __name__ == '__main__':
    app.run()
