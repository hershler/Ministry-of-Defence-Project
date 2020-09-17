import json
import csv
from operator import eq, ne, gt, lt, le, ge

import os
from pathlib import Path
from typing import Any, Dict, List
from dataclasses import dataclass
from dataclasses_json import dataclass_json

from db_api import DBTableBase, DB_ROOT, SelectionCriteria, DBField


@dataclass_json
@dataclass
class DBTable(DBTableBase):
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self, name: str, fields: List[DBField], key_field_name: str):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

        for i, field in enumerate(self.fields):
            if key_field_name == field.name:
                self.key_index = i

        if not Path(os.path.join(DB_ROOT, f"{self.name}_backup.json")).is_file():
            self.num_rows = 0
            self.indexes = {self.key_field_name: f"{self.name}_{self.key_field_name}_index.db"}
            self.__update_backup()

    def count(self) -> int:
        return self.num_rows

    def insert_record(self, values: Dict[str, Any]) -> None:

        new_record = [values.get(field.name) for field in self.fields
                      if isinstance(values.get(field.name), field.type)]
        if len(new_record) != len(self.fields):
            raise TypeError(f"fields' type don't match to the table" + self.name)

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'r') as table:
            reader = csv.reader(table)

            for row in reader:
                if row[self.key_index] == str(new_record[self.key_index]):
                    raise ValueError("The value of the key field already exists")

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'a', newline='') as table:
            writer = csv.writer(table)
            writer.writerow(new_record)

        self.num_rows += 1
        self.__update_backup()

    def delete_record(self, key: Any) -> None:
        rows = []
        exist = False
        with open(DB_ROOT.joinpath(self.name + '.csv'), 'r') as table:
            reader = csv.reader(table)
            for row in reader:
                if row and row[self.key_index] == str(key):
                    exist = True
                else:
                    rows.append(row)

        if not exist:
            raise ValueError("The value of the key field doesn't exists")

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'w') as table:
            writer = csv.writer(table)
            writer.writerows(rows)

        self.num_rows -= 1
        self.__update_backup()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        rows = []

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'r') as table:
            reader = csv.DictReader(table)

            for row in reader:
                if self.__satisfy(row, criteria):
                    self.num_rows -= 1
                else:
                    rows.append(row)

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'w', newline='') as table:
            writer = csv.DictWriter(table, fieldnames=[field.name for field in self.fields])
            writer.writeheader()
            writer.writerows(rows)

        self.__update_backup()

    def get_record(self, key: Any) -> Dict[str, Any]:

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'r') as table:
            reader = csv.DictReader(table)
            key_str = str(key)

            for row in reader:
                if key_str == row.get(self.key_field_name):
                    return row

            raise ValueError("key doesn't exist")

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'r') as table:
            rows = []
            key_str = str(key)
            reader = csv.DictReader(table)

            for row in reader:
                print(row)
                if row[self.key_field_name] == key_str:
                    exist = True
                    for k, v in values.items():
                        if row.get(k):
                            row[k] = v
                        else:
                            raise KeyError("key doesn't exist")
                rows.append(row)

        if not exist:
            raise ValueError("The value of the key field doesn't exists")

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'w', newline='') as table:
            writer = csv.DictWriter(table, fieldnames=[field.name for field in self.fields])
            writer.writeheader()
            writer.writerows(rows)

        self.__update_backup()

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        result = []

        with open(DB_ROOT.joinpath(self.name + '.csv'), 'r') as table:
            reader = csv.DictReader(table)

            for row in reader:
                if self.__satisfy(row, criteria):
                    result.append(row)
        return result

    def create_index(self, field_to_index: str) -> None:
        pass

    def reload_backup(self):
        with open(os.path.join(DB_ROOT, f"{self.name}_backup.json"), "r") as backup_file:
            meta_data = json.load(backup_file)
            self.num_rows = meta_data["num_rows"]
            self.indexes = meta_data["indexes"]

    def __update_backup(self):
        with open(os.path.join(DB_ROOT, f"{self.name}_backup.json"), "w") as backup_file:
            json.dump({"num_rows": self.num_rows,
                       "indexes": self.indexes}, backup_file)

    def __satisfy(self, item, criteria: List[SelectionCriteria]) -> bool:
        operator_dict = {"<": lt, ">": gt, "=": eq, "!=": ne, "<=": le, ">=": ge}

        for select in criteria:
            type_ = None
            key = select.field_name
            for filed in self.fields:
                if filed.name == key:
                    type_ = filed.type
            if not type_:
                raise ValueError("key not exists")
            first = type_(item[key])
            operator = select.operator
            value = select.value

            if not operator_dict[operator](first, value):
                return False
        return True
