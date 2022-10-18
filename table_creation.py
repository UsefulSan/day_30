import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Any
from main import format_tuple


def db_connect():
    """
    Соединяется с БД
    """
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="day_30")
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection
    except Error as error:
        print('Connection FAILED', error)


def handler(cursor: Any, request: Any):
    """
    Выполняет sql запросы
    """
    try:
        cursor.execute(request)
    except Error as err:
        print('request FAILED', err)


def create_table():
    """
    Создание всех таблиц по схеме
    :return: возвращает строку для sql
    """
    request = """
    CREATE TABLE IF NOT EXISTS type_dict(
    id_type SERIAL PRIMARY KEY,
    name_type VARCHAR(255));
    
    CREATE TABLE IF NOT EXISTS breed_dict(
    id_breed SERIAL PRIMARY KEY,
    name_breed VARCHAR(255));
    
    CREATE TABLE IF NOT EXISTS colour_dict(
    id_colour SERIAL PRIMARY KEY,
    name_colour VARCHAR(255));
    
    CREATE TABLE IF NOT EXISTS outcome_type(
    id_outcome_type SERIAL PRIMARY KEY,
    name_outcome_type VARCHAR(255));
    
    CREATE TABLE IF NOT EXISTS outcome_subtype(
    id_outcome_subtype SERIAL PRIMARY KEY,
    name_outcome_subtype VARCHAR(255));
    
    CREATE TABLE IF NOT EXISTS animal_dict(
    animal_id VARCHAR(10) NOT NULL PRIMARY KEY,
    fk_animal_type INTEGER REFERENCES type_dict(id_type),
    name VARCHAR(255),
    fk_breed INTEGER REFERENCES breed_dict(id_breed),
    fk_colour1 INTEGER REFERENCES colour_dict(id_colour),
    fk_colour2 INTEGER REFERENCES colour_dict(id_colour),
    date_of_birth TIMESTAMP(255));
    
    CREATE TABLE IF NOT EXISTS shelter_info(
    id_shelter_info VARCHAR(10) NOT NULL PRIMARY KEY,
    fk_animal_id VARCHAR(10) REFERENCES animal_dict(animal_id),
    fk_id_outcome_subtype INTEGER REFERENCES outcome_subtype(id_outcome_subtype),
    outcome_month INTEGER,
    outcome_year INTEGER,
    fk_outcome_type INTEGER REFERENCES outcome_type(id_outcome_type),
    age_upon_outcome VARCHAR(255));
    """
    return request


def insert_into(cursor, name_table: str, obj: tuple):
    """
    Добавлятор
    :param name_table: Название таблицы
    :param obj: кортеж объектов для добавления
    """
    if len(obj) == 1:
        values = f"{obj[0]}"
    else:
        values = str(obj)[1:-1].replace("'NULL'", "NULL").replace("\"", "'")
    req = f"""
          INSERT INTO {name_table}
          VALUES {values}
          """
    handler(cursor, req)


def insert_all(cursor, data_dict: dict):
    """
    Выполняет все необходимые добавления
    :param data_dict: Словарь данных
    """
    insert_into(cursor, "colour_dict", format_tuple(data_dict["colors"]))
    insert_into(cursor, "type_dict", format_tuple(data_dict["types"]))
    insert_into(cursor, "breed_dict", format_tuple(data_dict["breeds"]))
    insert_into(cursor, "outcome_subtype", format_tuple(data_dict["outcome_subtypes"]))
    insert_into(cursor, "outcome_type", format_tuple(data_dict["outcome_types"]))
    insert_into(cursor, "animal_dict", data_dict["animals"])
    insert_into(cursor, "shelter_info", data_dict["shelter"])


def new_user() -> str:
    """
    Создает новых пользователей и выдает им права
    :return: возвращает строку для sql
    """
    request = """
    CREATE USER test1 WITH ENCRYPTED PASSWORD 'password';
    GRANT SELECT ON ALL TABLES IN SCHEMA day_30 TO test1;

    CREATE USER test2 WITH ENCRYPTED PASSWORD 'password';
    GRANT INSERT ON ALL TABLES IN SCHEMA day_30 TO test2;
    GRANT UPDATE ON ALL TABLES IN SCHEMA day_30 TO test2;
    """
    return request
