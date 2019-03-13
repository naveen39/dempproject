from application import db
db.Model.metadata.reflect(db.engine)

from sqlalchemy.inspection import inspect


class Serializer(object):

    def serialize(self):
        return {c: getattr(self, c) for c in inspect(self).attrs.keys()}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]


class irsTOsfdcIntegration_year(db.Model, Serializer):

    __table__ = db.Model.metadata.tables['irsTOsfdcIntegration_year']
    
    def __init__(self,year_integer,Total_Records,Success_Records,Failure_Records,created_date,updated_date):

        self.year_integer = year_integer
        self.Total_Records = Total_Records
        self.Success_Records = Success_Records
        self.Failure_Records = Failure_Records
        self.created_date = created_date
        self.updated_date = updated_date
        
    def __repr__(self):
        return ' year_integer ' + str(self.year_integer) + ' Id ' + str(self.id)


class irsTOsfdcIntegration_account(db.Model, Serializer):

    __table__ = db.Model.metadata.tables['irsTOsfdcIntegration_account']

    '''def __init__(self, index,Year,ein,objectid,formtype,url,organization):
        
        self.index = index
        self.Year = Year
        self.ein = ein
        self.objectid = objectid
        self.formtype = formtype
        self.url = url
##        self.organization = organization
##        self.annual_revenue = annual_revenue
##        self.annual_expenses = annual_expenses
##        self.number_of_employees = number_of_employees
##        self.submittedon = submittedon
##        self.fiscal_year_end_date = fiscal_year_end_date
##        self.lastupdated = lastupdated
##        self.ready_to_sync = ready_to_sync
##        self.synced_to_sfdc = synced_to_sfdc
##        self.sfdc_id = sfdc_id
##        self.sfdc_status = sfdc_status
##        self.sfdc_err_msg = sfdc_err_msg
##        self.created_date = created_date
##        self.updated_date = updated_date'''
    def __init__(self, index,Year,ein,objectid,formtype,url,organization,annual_revenue,annual_expenses,number_of_employees,submittedon,fiscal_year_end_date,lastupdated,ready_to_sync,synced_to_sfdc,sfdc_id,sfdc_status,sfdc_err_msg,created_date,updated_date):
        
        self.index = index
        self.Year = Year
        self.ein = ein
        self.objectid = objectid
        self.formtype = formtype
        self.url = url
        self.organization = organization
        self.annual_revenue = annual_revenue
        self.annual_expenses = annual_expenses
        self.number_of_employees = number_of_employees
        self.submittedon = submittedon
        self.fiscal_year_end_date = fiscal_year_end_date
        self.lastupdated = lastupdated
        self.ready_to_sync = ready_to_sync
        self.synced_to_sfdc = synced_to_sfdc
        self.sfdc_id = sfdc_id
        self.sfdc_status = sfdc_status
        self.sfdc_err_msg = sfdc_err_msg
        self.created_date = created_date
        self.updated_date = updated_date


    def __repr__(self):
        return 'EIN ' + str(self.ein) + ' index ' + str(self.index)
