import db_api
import csv
from typing import Any, Dict, List
from db_api import DBTable, SelectionCriteria, DBField


class DataBase(db_api.DataBase):

    tables = {}  # Dict[str, DBTable]

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        exist = False
        for filed in fields:
            if filed.name == key_field_name:
                exist = True
        if not exist:
            raise ValueError("key filed does not exist")

        with open(db_api.DB_ROOT.joinpath(table_name + '.csv'), 'w') as table:
            writer = csv.writer(table)
            writer.writerow([field.name for field in fields])
        self.tables[table_name] = db_api.DBTable(table_name, fields, key_field_name)

        return self.tables[table_name]

    def num_tables(self) -> int:
        return self.tables.__len__()

    def get_table(self, table_name: str) -> DBTable:
        return self.tables.get(table_name)

    def delete_table(self, table_name: str) -> None:
        return self.tables.pop(table_name)

    def get_tables_names(self) -> List[Any]:
        return self.tables.keys()

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        return []
