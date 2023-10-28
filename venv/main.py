from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from datetime import datetime, timedelta
import csvparser, csv
import json, os, re
import pandas as pd
from threading import Timer

app = Flask(__name__)
api = Api(app)

def getProduct(self):    
        products = csvparser.productsFromCSV()
        json_string = json.dumps([ob.__dict__ for ob in products])
        final_dictionary = json.loads(json_string)
        return jsonify(final_dictionary)

def TransactionId(self, transactionId):
    transactions= csvparser.transactionsFromCSV()
    products = csvparser.productsFromCSV()
    
    for tx in transactions:
        if tx in transactions == transactionId:
            for prod in products:
                if prod.productId == tx.productId:
                    return jsonify({"transactionId" : tx.transactionId, "transactionAmount" : tx.transactionAmount, "transactionDatetime" : tx.transactionDatetime, "productId" : tx.productId, "productName" : prod.productName, "productManufacturingCity" : prod.productManufacturingCity})
            return jsonify({"error" : "parameter transaction Id : please enter valid value"})
        
        
def SummaryByProducts(self, lastNdays):
    transactions = csvparser.transactionsFromCSV()
    
    maxDate = lastRecentDate(transactions)
    dateNdaysAgo = maxDate - timedelta(days=lastNdays)
    prod = []
    
    for tx in transactions:
        if tx.txDate >= dateNdaysAgo:
            if updateProduct(prod, tx.productId, tx.transactionAmount) == False:
                prod.append({"productName" : productName(tx.productId), "totalAmount" : tx.transactionAmount})
            print(prod)
        return jsonify(summary = prod)

def productName(transactions):
    if len(transactions) > 0:
        lastRecentDate = transactions[0].txDate
        for tx in transactions:
            if tx.txDate > lastRecentDate:
                lastRecentDate = tx.txDate
        return lastRecentDate
    
def updateProduct(prod, productId, amount):
    for row in prod:
        if row["productName"] == productName(productId):
            row["totalAmount"] += amount
            return True
        return False
    
def productName(id):
    for obj in csvparser.productsFromCSV():
        if obj.productId == id:
            return obj.productName

def SummaryByManufacturingCity(self, lastndays):
        transactions = csvparser.transactionsFromCSV()
        
        maxDate = lastRecentDate(transactions)
        
        dateNdaysAgo = maxDate - timedelta(days=lastndays)
        
        prod = []
        for tx in transactions:
            if tx.txDate >= dateNdaysAgo:
                if updateProduct(prod, tx.productId, tx.transactionAmount) == False:
                    prod.append({"cityName" : cityName(tx.productId), "totalAmount" : tx.transactionAmount})
                print(prod)
            return jsonify(summary = prod)
    
def lastRecentDate(transactions):
        if len(transactions) > 0:
            lastRecentDate = transactions[0].txDate
            for tx in transactions:
                if tx.txDate > lastRecentDate:
                    lastRecentDate = tx.txDate
            return lastRecentDate
        
def updateProduct(prod, productId, amount):
        for row in prod:
            if row["cityName"] == cityName(productId):
                row["totalAmount"] += amount
                return True
            return False
    
def cityName(productId):
        products = csvparser.productsFromCSV()
        for product in products:
            if product.productId == productId:
                return product.productManufacturingCity

path = 'venv/csv/'

def retriveCSV(dir_path):
    inputfiles = [file for file in os.listdir(dir_path) if file.endswith('.csv')]
    listData = []
    for file in inputfiles:
        filepath = os.path.join(dir_path, file)
        listData.append(filepath)
    return listData
    
def productsFromCSV():
    products = []
    files = retriveCSV(path + 'ReferenceData/')
    for filepath in files:
        with open(filepath, "r") as csvfile:       
            reader = csv.DictReader(csvfile)        
            for row in reader:
                products.append(ProductModel(int(row['productId']), row['productName'], row['productManufacturingCity']))
    return products

def transactionsFromCSV():
    transactions = []
    files = retriveCSV(path + 'Transactions/')
    for filepath in files:
        with open(filepath, "r") as csvfile:       
            reader = csv.DictReader(csvfile)        
            for row in reader:
                transactions.append(TransactionModel(int(row['transactionId']), float(row['transactionAmount']), row['transactionDatetime'], int(row['productId'])))
    return transactions
    
class ProductModel:
    def __init__(self,productId,productName,productManufacturingCity):
        self.productId = productId
        self.productName = productName
        self.productManufacturingCity = productManufacturingCity
            
class TransactionModel:
    def __init__(self,transactionId,transAmount, transDatetime, productID):
        self.transactionId = transactionId
        self.productId = productID
        self.transactionAmount = transAmount
        self.transactionDatetime = transDatetime

    @property
    def txDate(self):
            return datetime.strptime(self.transactionDatetime, '%Y-%m-%d %H:%M:%S')

@app.route("/")
def home(): 
    return "Data Engineer - Coding Assignment"

@app.route("/sample", methods=["GET"])
def sample():
    products = productsFromCSV()
    json_string = json.dumps([ob.__dict__ for ob in products])
    final_dictionary = json.loads(json_string)
    return jsonify(final_dictionary)

@app.route("/assignment/transaction", methods=["GET"])
def trans():
    products = transactionsFromCSV()
    json_string = json.dumps([ob.__dict__ for ob in products])
    final_dictionary = json.loads(json_string)
    return jsonify(final_dictionary) 

@app.route("/assignment/transaction/<transactionId>", methods=["GET"])
def spec(transactionId):
    return TransactionId

@app.route("/assignment/transactionSummaryByProducts/<lastNdays>")
def summary_P(lastNdays):
    return SummaryByProducts

@app.route("/assignment/transactionSummaryByManufacturingCity/<lastndays>", methods=["GET"])    
def summary(lastndays):
    return SummaryByManufacturingCity

if __name__ == "__main__":
    app.run(port=8080,use_reloader=True,debug=True)