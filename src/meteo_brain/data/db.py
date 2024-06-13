# meteo_brain Weather prediction.
# Copyright (C) 2024  Alfred Taylor
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import time
import json
import pandas


class DBError(Exception):  # custom exception class to make error checking easier
    pass


EMPTY_CONFIG = {  # maybe this should be a local variable
    "date_of_creation": time.time(),
    "last_accessed": time.time(),
    "access_count": 0,
    "last_folder": "",
    "last_file": ""
}


class DataBase:
    __currentFolder = ""
    __currentFile = ""

    def __init__(self, source: str) -> None:  # check to ensure that there is a database in the specified path
        self.source = source

        if not os.path.exists(source) or len(os.listdir(source)) <= 0:
            self.__db_init()

    def __db_init(self):  # create conf file without adding any subfolders yet
        try:
            os.mkdir(self.source)
        except FileExistsError:
            pass

        with open(f"{self.source}/db_config.json", "w") as c:
            json.dump(EMPTY_CONFIG, c, indent=3)

    def __fs_init(self) -> None:  # start database file struct
        year = time.strftime("%Y")
        month = time.strftime("%b").lower()  # string month names for easy reading

        path = f"{self.source}/{year}/{month}/"  # potentially add folders for day of month too?
        os.makedirs(path)

        f_name = time.strftime("%a-%d-%H") + ".csv"
        open(f"{path}{f_name}", "a").close()  # why cant python have a nicer looking way to make files

        config = self.get_conf()
        config["last_folder"] = path
        config["last_file"] = f_name
        self.set_conf(config)

    def get_conf(self) -> dict:  # getter function for the config file
        try:
            with open(f"{self.source}/db_config.json", "r+") as c:
                config = json.load(c)
                self.__currentFolder = config["last_folder"]  # setting the globals (I hate globals)
                self.__currentFile = config["last_file"]
                return config

        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e}")

    def set_conf(self, content: dict) -> None:  # setter function for the config file
        try:
            with open(f"{self.source}/db_config.json", "w+") as c:
                self.__currentFolder = content["last_folder"]  # setting the globals (I hate globals)
                self.__currentFile = content["last_file"]
                c.truncate(0)
                json.dump(content, c, indent=3)

        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e}")

    def open(self) -> None:  # prepare db for use
        start = time.time()

        config = self.get_conf()
        config["last_accessed"] = time.time()
        config["access_count"] += 1
        self.set_conf(config)

        if self.__currentFolder == "" or self.__currentFile == "":  # only should run after __db_init
            self.__fs_init()

    def fetch(self) -> pandas.DataFrame:
        try:
            d = pandas.read_csv(f"{self.__currentFolder}{self.__currentFile}", sep=",", header=0)
        except pandas.errors.EmptyDataError as e:
            raise DBError(f"database file {self.__currentFile} invalid: {e}")
        return d
    def send(self, datavals):
        timestamp = time.time()

        return

