#  meteo_brain Weather prediction.
#  Copyright (C) 2024  Alfred Taylor
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
import os
import typing
import time
import json

class DBError(Exception):
    pass

class DataBase:
    __currentFolder = ""
    __currentFile = ""
    def __init__(self, source: str) -> None:
        self.source = source

        if not os.path.exists(source) or len(os.listdir(source)) <= 0:
            self.__create_db()
        else:
            self.__open_db()

    def __create_db(self):
        return

    def gen_conf(self):  # config file spec can be found in documentation
        return

    def __open_db(self):
        start = time.time()
        try:
            with open(f"{self.source}db_config.json", "r") as c_file:
                config = json.loads(c_file.read())
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e}")


        end = time.time()
        return

#DataBase("D:\\CompSciNEA\\meteo_brain\\tests\\example_db\\")




