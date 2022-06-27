import json
from turtle import pd
from urllib.request import urlopen
import urllib.error
import logging

from pymysql import connect, Error
import pandas as pd
import pandas.io.sql as sql
from sqlalchemy import create_engine, exc
from sqlalchemy.sql import text
from modules.utils import logger


class ForeignCurrencyRate():

    """From nbp(National Bank of Poland) API,  request JSON data that contains lates exchange rate to PLN for chosen currency in order to
    update unit prices for foreign currency

    Attributes:
        currency_code: str
            string with three-letter currency code (ISO 4217 standard) example ('usd', 'eur', ..) etc.
        table_name: str
            name of existing table in MySQL db
        column_name: str
            name of existing column or column  that will be created in MySQL db  with  create_foreign_UnitPrice_column() method
            that contains current unit price for chosen currency_code

        db_config: dict
            arguments to establish connection with MySQL database (pymysql.connect())

    Methods:

        get_currencies_rates(self) -> float

        update_foreign_unit_prices(self) -> None

        drop_column_from_table(self) -> None

        create_foreign_UnitPrice_column(self) -> None

        __str__(self) -> str

        """

    def __init__(
            self,
            currency_code: str,
            table_name: str,
            column_name: str,
            db_config: dict) -> None:
        self.currency_code = currency_code
        self.table_name = table_name
        self.column_name = column_name
        self.db_config = db_config
        self.currency_rate = None

    def get_currencies_rates(self) -> float:
        """returns last updated  rate,  for chosen currency_code  from nbp API """
        try:

            # API request
            url = f'http://api.nbp.pl/api/exchangerates/rates/a/{self.currency_code}/last/1/'
            req = urllib.request.Request(
                url, headers={'User-Agent': "Magic Browser"})
            response = urllib.request.urlopen(req)
            data_json = json.loads(response.read())

            # From JSON get currency rate
            cucurrency_rate = data_json['rates'][0]['mid']

        except urllib.error.HTTPError as err:
            return logger.error(f'Request Error {err.code}')

        return cucurrency_rate

    def update_foreign_unit_prices(self) -> None:
        """With exchange rate of foreign currency, updates column 'column_name' in table 'table_name'"""
        try:
            self.cucurrency_rate = self.get_currencies_rates()
            connection = connect(**self.db_config)

            try:

                cursor = connection.cursor()

                # Execute sql query to update columns
                with connection.cursor() as cursor:
                    sqlQuery = f"""
                            UPDATE {self.table_name}
                            SET {self.column_name} = UnitPrice/{self.cucurrency_rate}

                                """
                    cursor.execute(sqlQuery)
                    connection.commit()

                logger.info(
                    f'Unit price {self.currency_code} successfully updated !')
                connection.close()

            except Error as err:
                connection.close()
                return logger.error(f'Database update table problem:  {err}')
        except Error as err:
            return logger.error(f'Database connection problem:  {err}')

    def drop_column_from_table(self) -> None:
        """drops table based on 'column_name' and 'table_name' """
        try:
            connection = connect(**self.db_config)

            try:

                cursor = connection.cursor()

                # Execute sql query to add new foreign UnitPrice column
                with connection.cursor() as cursor:
                    sqlQuery = f"""
                                    ALTER TABLE {self.table_name}
                                    DROP COLUMN {self.column_name};
                                """
                    cursor.execute(sqlQuery)
                    connection.commit()

                logger.info(f'{self.column_name} dropped !')
                connection.close()

            except Error as err:
                connection.close()
                return logger.error(f'Database alter table problem:  {err}')
        except Error as err:
            return logger.error(f'Database connection problem:  {err}')

    def create_foreign_UnitPrice_column(self) -> None:
        """ Adds  extra column after UnitPrice with datatype decimal(10,0), meant to be UnitPrice(countrycode)"""
        try:
            connection = connect(**self.db_config)

            try:

                cursor = connection.cursor()

                # Execute sql query to add new foreign UnitPrice column
                with connection.cursor() as cursor:
                    sqlQuery = f"""
                                        ALTER TABLE {self.table_name}
                                        ADD COLUMN IF NOT EXISTS {self.column_name} DECIMAL(10,0) AFTER UnitPrice;
                                    """
                    cursor.execute(sqlQuery)
                    connection.commit()

                logger.info(f'{self.column_name} in db !')
                connection.close()

            except Error as err:
                connection.close()
                return logger.error(f'Database alter table problem:  {err}')
        except Error as err:
            return logger.error(f'Database connection problem:  {err}')

    def __str__(self) -> str:
        """show current rate for currency_code"""
        return f'''{self.currency_code}: {self.get_currencies_rates()} PLN'''


class CreateExcelSheetFromTable():
    """if file doesn't  exist  in /data directory, saves chosen table from MySQL db to xlsx file
       if file already exist,  with file name 'current_{table_name} it will be overwritten

    Attributes:
        table_name: str
            table that exist in MySQL db
        db_config: dict
            arguments to establish connection with MySQL database (to create sqlalchemy engine)
    Methods:
        create_excel_file_from_table -> None
    """

    def __init__(self, table_name: str, db_config: dict) -> None:
        self.table_name = table_name
        self.db_config = db_config

    def create_excel_file_from_table(self) -> None:
        try:
            engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                                   .format(user=self.db_config['user'],
                                           host=self.db_config['host'],
                                           pw=self.db_config['password'],
                                           db=self.db_config['db']))

            try:
                # Creates pd.DataFrame from chosen table and saves it to /data

                sql = f'''
                    SELECT * FROM {self.table_name};
                '''
                with engine.connect().execution_options(autocommit=True) as conn:
                    query = conn.execute(text(sql))

                df = pd.DataFrame(query.fetchall())

                if 'Picture' in df.columns:
                    df = df.drop(['Picture'], axis=1)
                df.to_excel(
                    f'data/current_{self.table_name}.xlsx',
                    index=False)
                logger.info(
                    f'Table "{self.table_name}" successfully saved to xlsx file !')
                engine.dispose()
            except exc.SQLAlchemyError as err:
                engine.dispose()
                return logger.error(f'Unable to save xlsx file:  {err}')

        except exc.SQLAlchemyError as err:
            return logger.error(f'Database connection problem:  {err}')
