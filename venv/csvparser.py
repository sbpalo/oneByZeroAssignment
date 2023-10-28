import csv
import json
import os
from datetime import datetime

def retriveCSV(dir_path):
    inputfiles = [file for file in os.listdir(dir_path) if file.endswith('.csv')]
    listData = []
    for file in inputfiles:
        filepath = os.path.join(dir_path, file)
        listData.append(filepath)
    return listData

BASE_PATH = 'venv/csv/'

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

def productsFromCSV():
    products = []
    files = retriveCSV(BASE_PATH + 'ReferenceData/')
    for filepath in files:
        with open(filepath, "r") as csvfile:       
            reader = csv.DictReader(csvfile)        
            for row in reader:
                products.append(ProductModel(int(row['productId']), row['productName'], row['productManufacturingCity']))
    return products
    
def transactionsFromCSV():
    transactions = []
    files = retriveCSV(BASE_PATH + 'Transactions/')
    for filepath in files:
        with open(filepath, "r") as csvfile:       
            reader = csv.DictReader(csvfile)        
            for row in reader:
                transactions.append(TransactionModel(int(row['transactionId']), float(row['transactionAmount']), row['transactionDatetime'], int(row['productId'])))
    return transactions

def toJson(csvFile): 
    json_from_csv = json.dumps(toList(csvFile))
    return json_from_csv

def toList(csvFile):    
    file = open(csvFile, "r")
    dict_reader = csv.DictReader(file)
    return list(dict_reader)

def toProducts(csvFile): 
    file = open(csvFile, "r")
    dict_reader = csv.DictReader(file)
    products = []
    for row in dict_reader:
        products.append(ProductModel(int(row['productId']), row['productName'], row['productManufacturingCity']))
    return products    