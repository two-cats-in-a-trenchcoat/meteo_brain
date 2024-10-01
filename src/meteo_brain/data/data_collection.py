# data collection for meteo_brain prediction algorithm
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

import threading

import requests

import db


class InputDaemon:
    def __init__(self, database: db.DataBase):
        self.database = database
        self.daemon = None

    def start(self, method, delay=10):
        self.daemon = threading.Thread(target=self.logging_daemon, args=(method, delay))
        self.daemon.start()

    def halt_low_priority(self):
        self.daemon.join()

    def logging_daemon(self, method, delay):
        pass


class DefualtMethods:
    def open_weather_map_API(APIKey: str, lat, lon, certificate=None):
        part = "minutely,hourly,daily,alerts"
        values = requests.get(
            f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={APIKey}",
            verify=certificate)
        return values


myDB = db.DataBase("D:\\CompSciNEA\\meteo_brain\\tests\\test_db\\", ["temperature", "humidity", "air quality"])
inputfetcher = InputDaemon(myDB)

cert = "D:\\CompSciNEA\\meteo_brain\\tests\\hcc.pem"

print(DefualtMethods.open_weather_map_API("54208e319a42531e68aa76aa3de4f0b0", "50.97", "0.26", cert))
