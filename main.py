#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymysql

from modules.repository import ForeignCurrencyRate, CreateExcelSheetFromTable


def main():

    # Parameters that establishes a connection to the MySQL database
    db_config = {'host': 'localhost',
                 'user': 'root',
                 'port': '',
                 'password': '',
                 'db': 'mydb',
                 'cursorclass': pymysql.cursors.DictCursor}

    # Init ForeignCurrencyRate objects
    usd_rate = ForeignCurrencyRate('USD', 'product', 'UnitPriceUSD', db_config)
    euro_rate = ForeignCurrencyRate(
        'EUR', 'product', 'UnitPriceEuro', db_config)


    # Update 'UnitPriceUSD' and 'UnitPriceEuro'in table 'product based on
    # 'UnitPrice' and current rate from nbp api '

    usd_rate.update_foreign_unit_prices()
    euro_rate.update_foreign_unit_prices()

    # print(usd_rate, euro_rate)

    # Create Excel file with chosen table in this example 'product'
    CreateExcelSheetFromTable(
        'product', db_config).create_excel_file_from_table()


if __name__ == '__main__':
    main()
