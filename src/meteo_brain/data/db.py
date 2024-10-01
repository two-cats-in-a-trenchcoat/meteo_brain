# database manager for meteo_brain
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

import pandas as pd


class DBError(Exception):  # custom exception class to make error checking easier
    pass


EMPTY_CONFIG: dict = {  # maybe this should be within DataBase's scope but doing that breaks it somehow
    "date_of_creation": time.time(),
    "last_accessed": time.time(),
    "access_count": 0,
    "last_folder": "",
    "last_file": ""
}

SECONDS_IN_A_DAY: int = 86400  # used when updating file names within db


class DataBase:
    """Object to represent the database.

    Interface with data structure through functions. Functionality for automated file structure management and repair of
    damage to database. Has search functions too.

    Attributes:
        source: str
            Location of preexisting database folder or folder to create database in.
        data_type_labels: list
            Labels for data when fetching and appending.

    Methods (public):
        get_conf():
            Returns settings contained within conf file as dict.
            Will raise a DBError if db_config.json is corrupt.
        set_conf(content: dict):
            Allows for manual manipulation of db_config.json within the database structure.
            Ensure that content is properly formatted, perhaps inherit from get_conf.
        fetch_current(filepath=None):
            Returns the values of the current db file as a pandas dataframe.
            Optionally, filepath can be specified to read from older files.
        fetch_historic(day_moth_year: tuple):
            Accepts a three entry tuple which contains an integer date.
            The database will be searched for a file on this date.
            Will raise FileNotFoundError if no file found at date specified.
        """
    __current_folder: str = ""
    __current_file: str = ""

    def __init__(self, source: str, data_type_labels: list) -> None:  # database checks it exists on init
        self.source = source
        self.empty_data_frame = pd.DataFrame(columns=data_type_labels)
        self.labels = data_type_labels

        if not os.path.exists(f"{source}/db_config.json") or len(os.listdir(source)) <= 0:
            self.__db_init()

        config = self.get_conf()
        config["last_accessed"] = time.time()
        config["access_count"] += 1
        self.set_conf(config)

        if self.__current_folder == "" or self.__current_file == "":  # only should run after __db_init has been run
            self.__fs_init()

    def __db_init(self) -> None:
        """Create conf file without db file structure."""
        if not os.path.exists(self.source):
            os.mkdir(self.source)

        with open(f"{self.source}/db_config.json", "w") as c:
            json.dump(EMPTY_CONFIG, c, indent=3)

    def __fs_init(self) -> None:
        """create full db structure and update conf file"""
        year: str = time.strftime("%Y")
        month: str = time.strftime("%b").lower()  # string month names for easy reading

        path: str = f"{self.source}/{year}/{month}/"  # potentially add folders for day of month too?

        if not os.path.exists(path):
            os.makedirs(path)

        f_name: str = f"{int(time.time())}-{time.strftime('%a-%d')}.csv"
        open(f"{path}{f_name}", "a").close()  # why cant python have a nicer looking way to make files

        config = self.get_conf()
        config["last_folder"] = path
        config["last_file"] = f_name
        self.set_conf(config)

    def __fs_check(self) -> None:
        """Function to check validity of currently initialised db, functionality for regenerating damage to db."""
        self.get_conf()
        year: str = time.strftime("%Y")
        month: str = time.strftime("%b").lower()
        path_keys: list = self.__current_folder.replace("\\", "/").replace("//", "/").split("/")
        while "" in path_keys:
            path_keys.remove("")

        file_dc: str = self.__current_file.split("-")[0]

        if path_keys[-1] != month and not os.path.exists(self.__current_folder):
            path: str = f"{self.source}/{year}/{month}/"
            os.makedirs(path)
            config = self.get_conf()
            f_name = self.__gen_file()
            config["last_folder"] = path
            config["last_file"] = f_name
            self.set_conf(config)

        if not os.path.exists(f"{self.__current_folder}{self.__current_file}") or abs(
                (int(file_dc) - int(time.time()))) >= SECONDS_IN_A_DAY:
            config = self.get_conf()
            f_name = self.__gen_file()
            config["last_file"] = f_name
            self.set_conf(config)

    def __gen_file(self) -> str:
        """Returns location of most recent file to be edited, if out of date creates new file."""
        path = self.__current_folder
        f_name: str = f"{int(time.time())}-{time.strftime('%a-%d')}.csv"
        self.empty_data_frame.to_csv(f"{path}{f_name}", sep=",", index=True)
        return f_name

    def get_conf(self) -> dict:
        """Getter for config data from config.json.

            Returns:
                Dict object that contains the contents of the JSON.
            Custom Exceptions:
                Will raise DBError if the config file is corrupt or poorly formatted.
            """
        # don't use __fs_check since it calls get_conf, so it cannot be used here.

        try:
            with open(f"{self.source}/db_config.json", "r+") as c:
                config = json.load(c)
                self.__current_folder = config["last_folder"]  # update private variables
                self.__current_file = config["last_file"]
                return config

        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e} ")

    def set_conf(self, content: dict) -> None:
        """allows manipulation of the db_config.json file. Updates internal private variables.

        Parameters:
            content: dict
                formatted dict containing the new values of the conf file
        Custom Exceptions:
            Will raise DBError if db_config.json does not exist.
        """

        # fs_check used to be here for some reason, DO NOT CALL fs_check within this scope!

        try:
            with open(f"{self.source}/db_config.json", "w+") as c:
                # reformat path to be more POSIX compliant
                content["last_folder"] = content["last_folder"].replace("\\", "/").replace("//", "/")
                self.__current_folder = content["last_folder"]  # update private variables
                self.__current_file = content["last_file"]
                c.truncate(0)  # clear file
                json.dump(content, c, indent=3)

        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise DBError(f"cannot load config: {e}")

    # with large file sizes running this function often is ill-advised, it eats ram. (to fix)
    def fetch_current(self, filepath=None) -> pd.DataFrame:
        """will fetch all the values of the current file that is being edited.

        Warning: will return a HUGE array if file is large.

        Parameters:
            filepath: str
                Optional specifier for specific file to get data from, by default will access most recent file.
        Returns:
            Pandas dataframe containing file data with column names inherited from self.source and timestamped rows.

        """
        # could there possibly be a way of only loading part of a file into ram?

        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        if not filepath:  # cannot accept self as a default argument so this check has to be made
            filepath: str = f"{self.__current_folder}{self.__current_file}"

        try:
            data: pd.DataFrame = pd.read_csv(filepath, sep=",", header=0, index_col=0)

        except (pd.errors.EmptyDataError, FileNotFoundError) as e:
            raise DBError(f"database file {self.__current_file} invalid: {e}")

        return data

    # fetch() but worse in every way
    def fetch_historic(self, day_month_year: Tuple[int, int, int]) -> Optional[pd.DataFrame]:
        """Search 
        """
        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        # non-discerning search algorithm, will search every file in database, room for speed improvements here
        # good place to implement binary search I think if I sort the date codes properly
        for root, dirs, files in os.walk(self.source):
            file_split: list = [x.split("-")[0] for x in files]

            for i, j in enumerate(file_split):  # absolutely horrible excuse for a search algorithm
                try:
                    # see if there is a better way to do this
                    if (time.localtime(int(j)).tm_mday == day_month_year[0] and
                            time.localtime(int(j)).tm_mon == day_month_year[1] and
                            time.localtime(int(j)).tm_year == day_month_year[2]):
                        return self.fetch_current(os.path.join(root, files[i]))
                except ValueError:  # this is also scuffed af >:(
                    pass

            raise FileNotFoundError("No file at date given.")  # may be better to return a flag instead of raising FNFE

    def commit_frame(self, data: pd.DataFrame) -> None:  # append multiple entries in the form of a dataframe
        self.__fs_check()  # best practice call __fs_check before directly manipulating the database

        # load file as DataFrame, modify dataframe, wipe file and then re-write.
        to_append: pd.DataFrame = self.fetch_current()

        if data.columns.empty:
            data.columns = self.labels

        open(f"{self.__current_folder}{self.__current_file}", "w").close()  # why, python, WHY!!!

        # merge the two DataFrames along their columns, if both share indexes this may cause issues
        if not to_append.empty:
            data = pd.concat([to_append, data])

        data.to_csv(f"{self.__current_folder}{self.__current_file}", mode="w")

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

        open(f"{self.__current_folder}{self.__current_file}", "w").close()  # this is such a wierd way to create a file

        data.to_csv(f"{self.__current_folder}{self.__current_file}", mode="w")
