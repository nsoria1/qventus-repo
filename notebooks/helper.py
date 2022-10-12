from array import array
import pandas as pd
from sqlalchemy import create_engine
import os

class Loader:
    def __init__(self, file: str, timestamp_cols: array = []) -> None:
        self.file = file
        self.timestamp_cols = timestamp_cols
        self.__set_tablename()

    def __set_tablename(self):
        print("Setting tablename attribute")
        if 'patient' in self.file:
            self.tablename = 'patient_data'
        elif 'procedure' in self.file:
            self.tablename = 'procedure_orders'

    @staticmethod
    def __connection_string() -> str:
        print("Getting connection string")
        return 'postgresql://data:data@postgres:5432/data'
    
    def __get_engine(self):
        print("Getting engine for writing into table")
        engine = create_engine(self.__connection_string())
        return engine

    def __read_csv(self) -> pd.DataFrame:
        try:
            print("Reading csv file from path")
            self.__set_data_path()
            self.df = pd.read_csv(self.data_path + self.file)
        except Exception as e:
            print(f"Couldn't load the file! Error is: {e}")
        else:
            if 'patient_data' in self.file:
                self.df = self.df.rename(columns={'admitting_ICD10': 'admitting_icd'})
    
    def __parse_dates(self) -> pd.DataFrame:
        print("Casting fields to timestamp")
        self.df[self.timestamp_cols] = self.df[self.timestamp_cols].apply(pd.to_datetime, errors='coerce')

    def __set_data_path(self) -> str:
        self.data_path = os.getcwd().rsplit('/', 1)[0] + '/data/'

    def load_file(self):
        self.get_df()
        self.df.to_sql(self.tablename, self.__get_engine(), if_exists="append", index=False)
        print("Writing to table!")

    def get_df(self) -> pd.DataFrame:
        try:
            self.__read_csv()
            if self.timestamp_cols:
                self.__parse_dates()
            return self.df
        except Exception as e:
            print(e)