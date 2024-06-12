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
import time
import json


class DBError(Exception):
    pass


EMPTY_CONFIG = {
    "date_of_creation": time.time(),
    "last_accessed": time.time(),
    "access_count": 0,
    "last_folder": "",
    "last_file": ""
}


class DataBase:
    __currentFolder = ""
    __currentFile = ""

    def __init__(self, source: str) -> None:
        self.source = source

        if not os.path.exists(source) or len(os.listdir(source)) <= 0:
            self.__create_db()

    def __create_db(self):
        try:
            os.mkdir(self.source)
        except FileExistsError:
            pass

        with open(f"{self.source}db_config.json", "w") as c:
            json.dump(EMPTY_CONFIG, c, indent=3)

    def open(self):
        start = time.time()
        try:
            with open(f"{self.source}db_config.json", "r+") as c:
                config = json.load(c)
                c.truncate(0)

            config["last_accessed"] = time.time()
            config["access_count"] += 1
            self.__currentFolder = config["last_folder"]
            self.__currentFile = config["last_file"]

            with open(f"{self.source}db_config.json", "r+") as c:
                json.dump(config, c, indent=3)

        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e}")

        end = time.time()
        return


DataBase("D:\\CompSciNEA\\meteo_brain\\tests\\test_db\\").open()
