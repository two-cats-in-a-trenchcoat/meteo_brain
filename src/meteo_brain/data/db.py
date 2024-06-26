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

# python stdlib moment
import json
import os
import time
import webbrowser
from typing import *

# pd is by itself because it is a naughty dependency that takes ages to initialise
import pandas as pd


class DBError(Exception):  # custom exception class to make error checking easier
    pass


EMPTY_CONFIG: dict = {  # maybe this should be within DataBase's scope
    "date_of_creation": time.time(),
    "last_accessed": time.time(),
    "access_count": 0,
    "last_folder": "",
    "last_file": ""
}

SECONDS_IN_A_DAY: int = 86400


class DataBase:
    __currentFolder: str = ""
    __currentFile: str = ""

    def __init__(self, source: str, sensor_labels: list) -> None:  # database checks it exists on init
        self.source = source
        self.empty_data_frame = pd.DataFrame(columns=sensor_labels)
        self.labels = sensor_labels

        if not os.path.exists(f"{source}/db_config.json") or len(os.listdir(source)) <= 0:
            self.__db_init()

        config = self.get_conf()
        config["last_accessed"] = time.time()
        config["access_count"] += 1
        self.set_conf(config)

        if self.__currentFolder == "" or self.__currentFile == "":  # only should run after __db_init has been run once
            self.__fs_init()

    def __db_init(self) -> None:  # create conf file without adding any subfolders yet
        try:
            os.mkdir(self.source)
        except FileExistsError:
            pass

        with open(f"{self.source}/db_config.json", "w") as c:
            json.dump(EMPTY_CONFIG, c, indent=3)

    def __fs_init(self) -> None:  # same functionality as fs_check but allows for an empty patch
        year: str = time.strftime("%Y")
        month: str = time.strftime("%b").lower()  # string month names for easy reading

        path: str = f"{self.source}/{year}/{month}/"  # potentially add folders for day of month too?

        try:
            os.makedirs(path)
        except FileExistsError:
            pass

        f_name: str = f"{int(time.time())}-{time.strftime('%a-%d')}.csv"
        open(f"{path}{f_name}", "a").close()  # why cant python have a nicer looking way to make files

        config = self.get_conf()
        config["last_folder"] = path
        config["last_file"] = f_name
        self.set_conf(config)

    def __fs_check(self) -> None:  #
        self.get_conf()
        year: str = time.strftime("%Y")
        month: str = time.strftime("%b").lower()

        path_keys: list = self.__currentFolder.replace("\\", "/").replace("//", "/").split("/")
        while "" in path_keys:
            path_keys.remove("")

        file_dc: str = self.__currentFile.split("-")[0]

        if path_keys[-1] != month or not os.path.exists(self.__currentFolder):
            path: str = f"{self.source}/{year}/{month}/"
            os.makedirs(path)
            config = self.get_conf()
            f_name = self.__gen_file()
            config["last_folder"] = path
            config["last_file"] = f_name
            self.set_conf(config)

        if not os.path.exists(f"{self.__currentFolder}{self.__currentFile}") or abs(
                (int(file_dc) - int(time.time()))) >= SECONDS_IN_A_DAY:
            config = self.get_conf()
            f_name = self.__gen_file()
            config["last_file"] = f_name
            self.set_conf(config)

    def __gen_file(self) -> str:  # generate files as time changes
        path = self.__currentFolder
        f_name: str = f"{int(time.time())}-{time.strftime('%a-%d')}.csv"
        self.empty_data_frame.to_csv(f"{path}{f_name}", sep=",", index=True)
        return f_name

    def get_conf(self) -> dict:  # getter function for the config file
        # __fs_check calls get_conf, so it cannot be used here.

        try:
            with open(f"{self.source}/db_config.json", "r+") as c:
                config = json.load(c)
                self.__currentFolder = config["last_folder"]  # update private variables
                self.__currentFile = config["last_file"]
                return config

        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e} ")

    def set_conf(self, content: dict) -> None:  # setter function for the config file
        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        try:
            with open(f"{self.source}/db_config.json", "w+") as c:
                # reformat path to be POSIX compliant
                content["last_folder"] = content["last_folder"].replace("\\", "/").replace("//", "/")
                self.__currentFolder = content["last_folder"]  # update private variables
                self.__currentFile = content["last_file"]
                c.truncate(0)  # clear file
                json.dump(content, c, indent=3)

        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e}")

    # with large file sizes running this function often is ill-advised, it eats ram. Perhaps cache values before commit?
    def fetch_current(self, filepath=None) -> pd.DataFrame:
        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        if filepath is None:  # cannot accept self as a default argument so this check has to be made
            filepath: str = f"{self.__currentFolder}{self.__currentFile}"

        try:
            data: pd.DataFrame = pd.read_csv(filepath, sep=",", header=0, index_col=0)

        except (pd.errors.EmptyDataError, FileNotFoundError) as e:
            raise DBError(f"database file {self.__currentFile} invalid: {e}")

        return data

    # this func is the same as fetch() but slower...
    def fetch_historic(self, day_month_year: Tuple[int, int, int]) -> Optional[pd.DataFrame]:
        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        # non-discerning search algorithm, will search every file in database, room for speed improvements here
        for root, dirs, files in os.walk(self.source):
            file_split: list = [x.split("-")[0] for x in files]

            for i, j in enumerate(file_split):
                try:
                    # see if there is a better way to do this
                    if (time.localtime(int(j)).tm_mday == day_month_year[0] and
                            time.localtime(int(j)).tm_mon == day_month_year[1] and
                            time.localtime(int(j)).tm_year == day_month_year[2]):
                        return self.fetch_current(os.path.join(root, files[i]))
                except ValueError:  # this is also scuffed as fuck
                    pass

    def commit_frame(self, data: pd.DataFrame) -> None:  # append multiple entries in the form of a dataframe
        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        # load file as DataFrame, modify dataframe, wipe file and then re-write.
        to_append: pd.DataFrame = self.fetch_current()

        if data.columns.empty:
            data.columns = self.labels

        open(f"{self.__currentFolder}{self.__currentFile}", "w").close()  # why, python, WHY!!!

        # merge the two DataFrames along their columns, if both share indexes this may cause issues
        if not to_append.empty:
            data = pd.concat([to_append, data])

        data.to_csv(f"{self.__currentFolder}{self.__currentFile}", mode="w")

    def display(self, dataset: pd.DataFrame) -> None:  # display current db file as html
        url: str = f"{self.source}/tmp.html"
        dataset.to_html(url)
        webbrowser.open(url)
        os.remove(url)

    def commit_item(self, data_item: List[int]) -> None:  # add only one line of data to current database file
        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        # load file as DataFrame, modify dataframe, wipe file and then re-write.
        data = self.fetch_current()
        data.loc[len(data)] = data_item

        open(f"{self.__currentFolder}{self.__currentFile}", "w").close()  # why, python, WHY!!!

        data.to_csv(f"{self.__currentFolder}{self.__currentFile}", mode="w")
