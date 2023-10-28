from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from datetime import datetime, timedelta
import csvparser, csv
import json, os, re
import pandas as pd
from threading import Timer

app = Flask(__name__)
api = Api(app)

class Store:
    def __init__(self):
        self.transactions = []
        self.products = []
        self.dFrameTransactions = pd.DataFrame(self.transactions)
        self.dFrameProducts = pd.DataFrame(self.products)
        
    def getProductNameById(self, productId):
        productDetails = self.dframeProducts[self.dframeProducts['productId'] == productId]
        productDetails = json.loads(productDetails.iloc[0].to_json())
        return productDetails['productName']
    
    def getTransactionById(self, transactionId):
        try: 
            tdata = self.dFrameTransactions[self.dFrameTransactions['transactionId'] == transactionId]
            response = json.loads(tdata.iloc[0].to_json())
            response['productName'] = self.getProductNameById(response['productId'])
            return jsonify(response)
        except IndexError as error:
            return jsonify({"error": "parameter transaction Id : please enter valid value"})
        except ValueError as error:
            return jsonify({"error": "parameter transaction Id : please enter a valid Integer value"})

        
    def getManCityNameById(self, productId):
            productDetails = self.dFrameProducts[(self.dFrameProducts['productId'] == productId)]
            productDetails = json.loads(productDetails.iloc[0].to_json())
            return productDetails['productManufacturingCity']

    def transactionSummaryByProducts(self, lastndays):
            startDate = pd.to_datetime(datetime.today() - timedelta(days=lastndays))
            endDate = pd.to_datetime(datetime.now())
            resultset = self.dFrameTransactions[
                (self.dFrameTransactions['transactionDatetime'] >= startDate) & (self.dFrameTransactions['transactionDatetime'] <= endDate)].groupby(
                'productId')[['transactionAmount']].sum()

            responseArray = []

            for indx, row in resultset.iterrows():
                respObj = {}
                respObj['productName'] = self.getProductNameById(indx)
                respObj['totalAmount'] = float(row['transactionAmount'])
                responseArray.append(respObj)

            return jsonify({'summary': responseArray})

    def transactionSummaryByManufacturingCity(self, lastndays):
        startDate = pd.to_datetime(datetime.today() - timedelta(days=lastndays))
        endDate = pd.to_datetime(datetime.now())
        transet = self.dFrameTransactions[(self.dFdFrameTransactionsrame['transactionDatetime'] > startDate) & (self.dFrameTransactions['transactionDatetime'] < endDate)]

        responseArray = []

        for indx, row in transet.iterrows():
            respObj = {}
            respObj['cityName'] = self.getManCityNameById(int(row['productId']))
            respObj['transactionAmount'] = float(row['transactionAmount'])
            responseArray.append(respObj)

        resultset = pd.DataFrame(responseArray)

        responseArray = []
        if resultset.empty:
            return jsonify({'summary': responseArray})
        else:
            resultset = resultset.groupby('cityName')[['transactionAmount']].sum()

            print(resultset)

            for indx, row in resultset.iterrows():
                respObj = {}
                respObj['cityName'] = indx
                respObj['totalAmount'] = float(row['transactionAmount'])
                responseArray.append(respObj)

            return jsonify({'summary': responseArray})
        
    def loadProducts(self):
        path = 'venv/csv/ReferenceData/'
        tempProd = []
        
        for filename in os.listdir(path):
            if filename == "ProductReference.csv":
                with open(os.path.join(path, filename), 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        tempobj = dict(row)
                        tempobj['productId'] = row['productId']
                        tempobj['productName'] = row['productName']
                        tempobj['productManufacturingCity'] = row['productManufacturingCity']
                        tempProd.append(tempobj)
                self.products = tempProd
        self.dframeProducts = pd.DataFrame(self.products)
        return self.dframeProducts, self.products

    def loadtrans(self):
            path = "venv/csv/TransactionData/"
            temptransactions = []

            for filename in os.listdir(path):
                if re.match(r"^Transaction_*", filename):
                    with open(os.path.join(path, filename), 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            tempobj = dict(row)
                            tempobj['transactionId'] = int(tempobj['transactionId'])
                            tempobj['productId'] = int(tempobj['productId'])
                            tempobj['transactionAmount'] = float(tempobj['transactionAmount'])
                            tdatetime = datetime.strptime(tempobj['transactionDatetime'], "%d/%m/%Y %H:%M").strftime(
                                "%Y-%m-%d %H:%M:%S")
                            tempobj['transactionDatetime'] = pd.to_datetime(tdatetime)
                            # print(tempobj['transactionDatetime'] .strftime("%d"))
                            # print(tempobj['transactionDatetime'].strftime("%m"))
                            # print(tempobj['transactionDatetime'].strftime("%Y"))
                            temptransactions.append(tempobj)
                    print(filename)
            self.transactions = temptransactions
            self.dFrame = pd.DataFrame(self.transactions)
            return self.dFrame, self.transactions

    def loaddata(self):
        path ="venv/csv/TransactionData/"
        temptransactions = []
        tempProd = []

        for filename in os.listdir(path):
            if re.match(r"^Transaction_*", filename):
                        with open(os.path.join(path, filename), 'r') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                tempobj = dict(row)
                                tempobj['transactionId'] = int(tempobj['transactionId'])
                                tempobj['productId'] = int(tempobj['productId'])
                                tempobj['transactionAmount'] = float(tempobj['transactionAmount'])
                                tdatetime = datetime.strptime(tempobj['transactionDatetime'], "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
                                tempobj['transactionDatetime'] = pd.to_datetime(tdatetime)
                                #print(tempobj['transactionDatetime'] .strftime("%d"))
                                #print(tempobj['transactionDatetime'].strftime("%m"))
                                #print(tempobj['transactionDatetime'].strftime("%Y"))
                                temptransactions.append(tempobj)
                        print(filename)
        self.transactions = self.transactions.append(temptransactions)

        self.dFrameTransactions = pd.DataFrame(self.transactions)

            # init variables
        return self.dFrameTransactions, self.transactions, self.products, self.dFrameProducts

store = Store()


class TReload:

    def __init__(self, t, Function):
        self.t = t
        self.hFunction = Function
        self.thread = Timer(self.t, self.hFunction)

    def start(self):
        self.thread.start()

def refresh():
    endtime = datetime.now()
    starttime = (datetime.now() - timedelta(minutes=5))
    print('before refreshing')
    store.loaddata(starttime,endtime)
    print('after refreshing the database')

@app.route("/")
def home(): 
    return "Data Engineer - Coding Assignment"

@app.route("/assignment/transaction/<transactionId>", methods=["GET"])
def spec(transactionId):
        return store.getTransactionById(int(transactionId))

@app.route("/assignment/transaction/SummaryByProducts/<lastDays>", methods=["GET"])
def summary_p(lastDays):
    return store.transactionSummaryByProducts(int(lastDays))


@app.route("/assignment/transaction/SummaryByManufacturingCity/<lastDays>", methods=["GET"])
def summary_c(lastDays):
    return store.transactionSummaryByManufacturingCity(int(lastDays))


if __name__ == "__main__":
    app.run(port=8080, debug=True)

    store.loadproducts()
    store.loadtrans()
    t = TReload(300, refresh)
    t.start()
    app.run(port=8080,use_reloader=False,debug=True)
    #app.run(port=8080, host="0.0.0.0", use_reloader=False,debug=True)host="0.0.0.0"