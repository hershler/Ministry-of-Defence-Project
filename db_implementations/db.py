from pathlib import Path
import csv
from typing import Any, Dict, List
from db_api import DataBaseBase, SelectionCriteria, DBField, DB_ROOT
from db_implementations.db_table import DBTable
import os
import json


class DataBase(DataBaseBase):

    __tables: Dict[str, DBTable]

    def __init__(self):
        self.__tables = dict()

        if not Path(f"{ DB_ROOT }/DB.json").is_file():
            self.__update_backup()
        else:
            self.__reload_backup()

        for table in self.__tables.values():
            table.reload_backup()

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        if Path(DB_ROOT.joinpath(table_name + '.csv')).is_file():
            print(f" { table_name } table already exists")
            self.__reload_backup()
        else:
            exist = False

            for filed in fields:
                if filed.name == key_field_name:
                    exist = True
            if not exist:
                raise ValueError("key field does not exist")

            with open(DB_ROOT.joinpath(table_name + '.csv'), 'w', newline='') as table:
                writer = csv.writer(table)
                writer.writerow([field.name for field in fields])
        self.__tables[table_name] = DBTable(table_name, fields, key_field_name)
        self.__update_backup()
        return self.__tables[table_name]

    def num_tables(self) -> int:
        return len(self.__tables)

    def get_table(self, table_name: str) -> DBTable:
        try:
            return self.__tables[table_name]
        except KeyError:
            raise NameError("this table doesnt exist")

    def delete_table(self, table_name: str) -> None:
        try:
            self.__tables.pop(table_name)
            try:
                os.remove(DB_ROOT.joinpath(table_name + '.csv'))
                os.remove(DB_ROOT.joinpath(table_name + '_backup.json'))
            except FileNotFoundError:
                print("something got wrong...")
        except KeyError:
            raise NameError("this table doesnt exist")

        self.__update_backup()

    def get_tables_names(self) -> List[Any]:
        return list(self.__tables.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        return []

    def __reload_backup(self):
        with open(os.path.join(DB_ROOT, "DB.json"), "r") as backup_file:
            tables = json.load(backup_file)

            for name, table in tables.items():

                if not self.__tables.get(name):
                    self.__tables[name] = DBTable(name,
                                                  [DBField(field[0], field[1]) for field in table["fields"]],
                                                  table["key_field_name"])

                self.__tables[name].num_rows = table["num_rows"]
                self.__tables[name].indexes = table["indexes"]

    def __update_backup(self):

        with open(os.path.join(DB_ROOT, "DB.json"), "w") as backup_file:
            tables = {t_name: {"fields": [(field.name, str(field.type)) for field in table.fields],
                               "key_field_name": table.key_field_name,
                               "num_rows": table.num_rows,
                               "indexes": table.indexes}
                      for t_name, table in self.__tables.items()}
            json.dump(tables, backup_file)
