from flask import Flask,request
import requests

#from models import db

#from flask_sqlalchemy import SQLAlchemy

#from models import *
#import datetime,time
#from timeit import default_timer as timer 

app = application = Flask(__name__)



# app.config['DEBUG'] = True
# app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://irsadmin:admin369@irsinstance.czuzdoyxg3ji.us-east-1.rds.amazonaws.com:5432/ebdb'
# #db.init_app(app)

# db = SQLAlchemy(app)
# db.Model.metadata.reflect(db.engine)

# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

@app.route("/")
def main():
    return 'Hello World !'



    
@app.route('/tords', methods=['GET', 'POST'])
def tords():
    print('--------------scheduler started')
    return '--------------scheduler started'
    

@app.route('/tords2')
def tords2():
    print('----------strted--')
    response = requests.get('https://s3.amazonaws.com/irs-form-990/index_2018.json')
    d = response.json()
    processing_year_res="Filings2018"

    for x in range(0,10):
        #print(d[x]['EIN'])
        print(d[processing_year_res][x]['EIN'])
        
        return 'success'

if __name__ == '__main__':
    app.run(debug=False)
