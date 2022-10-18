import csv

from table_creation import *


def read_file() -> list:
    """
    Чтение файла и добавление его строк в список
    :return: список строк
    """
    with open('main_animals.csv', newline='', encoding='utf-8') as file:
        list_data = []
        file_reader = csv.DictReader(file)
        for row in file_reader:
            list_data.append(row)
        return list_data


def add_to_set(set_: set, obj):
    """
    Добавляет в множество только непустые элементы
    """
    if obj:
        set_.add(obj.strip())


def filter_data(data_list: list) -> dict:
    """
    Разбивает основной список с данными на маленькие и записывает нужные в словарь с кортежами
    :param data_list: Основной список
    :return: Словарь кортежей
    """
    colors = set()
    breeds = set()
    types = set()
    outcome_types = set()
    outcome_subtypes = set()
    for row in data_list:
        add_to_set(colors, row["color1"])
        add_to_set(colors, row["color2"])
        add_to_set(breeds, row["breed"])
        add_to_set(types, row["animal_type"])
        add_to_set(outcome_types, row["outcome_type"])
        add_to_set(outcome_subtypes, row["outcome_subtype"])
    return {"colors": tuple(colors), "breeds": tuple(breeds), "types": tuple(types),
            "outcome_types": tuple(outcome_types), "outcome_subtypes": tuple(outcome_subtypes)}


def split_complex_data(data_list: list, data_dict: dict):
    """
    Разделяет основной список на два больших
    :param data_list: Основной список
    :param data_dict: Словарь для записи результата
    """
    animals = []
    shelter = []
    for row in data_list:
        animals.append((row["animal_id"], row["animal_type"], row["name"], row["breed"], row["color1"], row["color2"],
                        row["date_of_birth"]))
        shelter.append((row["index"], row["animal_id"], row["outcome_subtype"], row["outcome_month"],
                        row["outcome_year"], row["outcome_type"], row["age_upon_outcome"]))
    data_dict["animals"] = tuple(set(animals))
    data_dict["shelter"] = tuple(shelter)


def format_tuple(start_tuple: tuple) -> tuple:
    """
    Форматирует кортеж под sql запрос
    :param start_tuple: Кортеж для форматирования
    :return: Результирующий кортеж
    """
    list_ = []
    for i in range(len(start_tuple)):
        list_.append((i + 1, start_tuple[i]))
    return tuple(list_)


def redact_list(origin_list: list, data_dict: dict):
    """
    Заменяет в списке значения на индексы и форматирует некоторые значения под sql запрос
    :param origin_list: Список данных
    :param data_dict: Словарь с кортежами для получения индексов
    """
    for row in origin_list:
        row["animal_type"] = data_dict["types"].index(row["animal_type"].strip()) + 1
        row["breed"] = data_dict["breeds"].index(row["breed"].strip()) + 1
        row["color1"] = data_dict["colors"].index(row["color1"].strip()) + 1
        if row["color2"]:
            row["color2"] = data_dict["colors"].index(row["color2"].strip()) + 1
        else:
            row["color2"] = "NULL"
        if row["outcome_subtype"]:
            row["outcome_subtype"] = data_dict["outcome_subtypes"].index(row["outcome_subtype"].strip()) + 1
        else:
            row["outcome_subtype"] = "NULL"
        if row["outcome_type"]:
            row["outcome_type"] = data_dict["outcome_types"].index(row["outcome_type"].strip()) + 1
        else:
            row["outcome_type"] = "NULL"
        row["name"] = row["name"].replace("'", "''")



def main():
    data_list = read_file()
    data_dict = filter_data(data_list)
    redact_list(data_list, data_dict)
    split_complex_data(data_list, data_dict)
    connection = db_connect()
    cursor = connection.cursor()
    handler(cursor, create_table())
    insert_all(cursor, data_dict)
    handler(cursor, new_user())
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
