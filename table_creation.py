import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Any


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
        print('Connection DONE')
        return connection
    except Error as error:
        print('Connection FAILED', error)
        return False


def handler(cursor: Any, request: Any):
    try:
        cursor.execute(request)
    except Error as err:
        print('request FAILED', err)


def create_table():
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
    animal_id INTEGER NOT NULL PRIMARY KEY,
    fl_animal_type INTEGER REFERENCES type_dict(id_type),
    name VARCHAR(255),
    fk_breed INTEGER REFERENCES breed_dict(id_breed),
    fk_colour1 INTEGER REFERENCES colour_dict(id_colour),
    fk_colour2 INTEGER REFERENCES colour_dict(id_colour),
    date_of_birth TIMESTAMP(255));
    
    CREATE TABLE IF NOT EXISTS Shelter_info(
    id_shelter_info INTEGER NOT NULL PRIMARY KEY,
    fk_animal_id INTEGER REFERENCES animal_dict(animal_id),
    fk_id_outcome_subtype INTEGER REFERENCES outcome_subtype(id_outcome_subtype),
    outcome_month INTEGER,
    outcome_year INTEGER,
    fk_outcome_type INTEGER REFERENCES outcome_type(id_outcome_type),
    age_upon_outcome VARCHAR(255));
    """
    return request
