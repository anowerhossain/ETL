import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import json

file = open('credential.JSON','r')
x = file.read()
file_data = json.loads(x)

live_ip = file_data['liveIP']
live_db = file_data['livedb']
live_user = file_data['liveuser']
live_pass = file_data['live_password']
live_port = file_data['live_port']


dwh_ip = file_data['dwhIP']
dwh_db = file_data['dwhdb']
dwh_user = file_data['dwhuser']
dwh_pass = file_data['dwh_password']



class Credential:
    def live_connection(self):
        try:
            self.__live_connection = mysql.connector.connect(host=live_ip,
                                                             database=live_db,
                                                             user=live_user,
                                                             password=live_pass,
                                                             port=live_port
                                             )
            if self.__live_connection.is_connected():
                print("Connection Established With MySQL (Live)")
            return self.__live_connection
        except Error as e:
            print("Error while connecting to MySQL", e)
    def dwh_connection_engine(self):
        try:
            self.__engine = create_engine("postgresql://{user}:{pw}@{ip}/{db}"
                                   .format(ip=dwh_ip,
                                           user=dwh_user,
                                           pw=dwh_pass,
                                           db=dwh_db))
            return self.__engine
        except Error as e:
            print("Error while connecting to DWH", e)

