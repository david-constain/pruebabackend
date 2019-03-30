import pandas as pd
import sqlite3
from pandas import ExcelWriter
from pandas import ExcelFile
from sqlalchemy import create_engine

#6.1
def countrydataset(data):
    countries = data['Country'].drop_duplicates()
    for country in countries:
        countrydf = data[data['Country'] == country]
        countrydf.to_excel('{}.xlsx'.format(country), engine='xlsxwriter')

#6.2
def unitpricepercustomer(data):
    months = pd.Series(pd.DatetimeIndex(data['InvoiceDate']).month).rename('Mes')
    pricepcustomer = pd.concat([data, months], axis=1).drop(columns = 'InvoiceDate')
    pricepcustomer = pd.DataFrame(pricepcustomer.groupby(['Mes','CustomerID'])['UnitPrice'].sum()).sort_values(by='UnitPrice', ascending=False).round(3)
    pricepcustomer.to_csv('unitpricepercustomer.csv', sep='\t', encoding='utf-8')

#6.3
def insertproducts(data):
    engine = create_engine('sqlite://', echo=False)
    sqlite_file = 'onlineretaildb.sqlite'
    table_name = 'onlineretail' 
    product_field = 'product' 
    product_type = 'TEXT'
    country_field = 'country'
    country_type = 'TEXT'
    quantity_field = 'quantity'
    quantity_type = 'INTEGER'

    connection = sqlite3.connect(sqlite_file)
    conn = connection.cursor()

    engine.execute('CREATE TABLE IF NOT EXISTS {tn} ({pf} {pt}, {cf} {ct}, {qf} {qt})'\
            .format(tn=table_name, pf=product_field, pt=product_type, cf=country_field, ct=country_type, qf=quantity_field, qt=quantity_type))

    products = data[data['Country'].isin(['United Kingdom', 'France'])]
    descriptions = products['Description'].drop_duplicates()
    productspercus = pd.DataFrame(products.groupby(['Country','Description'])['Description'].count().rename('Quantity'))

    productspercus.to_sql('onlineretail', con=engine, if_exists='append', index=False)
    engine.execute("SELECT * FROM onlineretail").fetchall()

    connection.commit()
    connection.close()

    

if __name__ == '__main__':
    sheetname = 'Online Retail'
    file_name = 'Online Retail.xlsx'
    excelsource = pd.read_excel(file_name, sheet_name = sheetname)
    exceldataframed = pd.DataFrame(excelsource)
    countrydataset(exceldataframed)
    unitpricepercustomer(exceldataframed)
    insertproducts(exceldataframed)

