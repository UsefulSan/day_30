import csv

from table_creation import *


def read_file():
    with open('main_animals.csv', newline='', encoding='utf-8') as file:
        list_data = []
        file_reader = csv.DictReader(file)
        for row in file_reader:
            list_data.append(row)
        return list_data


def set_():
    pass


def main():
    data_list = read_file()
    connection = db_connect()
    cursor = connection.cursor()
    table = create_table()
    handler(cursor, table)


if __name__ == '__main__':
    main()
