import psycopg2
from psycopg2.extensions import AsIs

def create_database(cur):
	"""Создаёт таблицы client и phone_number.
	
	Входные параметры: 
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor

	"""

	cur.execute("""
		CREATE TABLE IF NOT EXISTS client(
			id SERIAL PRIMARY KEY,
			first_name VARCHAR(50) NOT NULL,
			last_name VARCHAR(50) NOT NULL,
			email VARCHAR(100) UNIQUE
		);
	""")

	cur.execute("""
		CREATE TABLE IF NOT EXISTS phone_number(
			id SERIAL PRIMARY KEY,
			client_id INTEGER NOT NULL REFERENCES client(id),
			number VARCHAR(20) NOT NULL
		);
	""")


def add_client(cur, first_name: str, last_name: str, email: str='') -> int:
	""" Добавляет все данные клиента в таблицу client.

	Входные параметры:
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor
	first_name - имя клиента. Не допускается длина более 50 (однобайтовых) символов
	last_name - фамилия клиента. Не допускается длина более 50 (однобайтовых) символов
	email - электронная почта клиента. Пустая строка по умолчанию

	Возвращаемое значение:
	В случае успешного добавления строки в таблицу client будет возвращен целочисленный id нового клиента.

	"""

	cur.execute("""
		INSERT INTO client(first_name, last_name, email)
		VALUES(%s, %s, %s)
		RETURNING id;
	""", (first_name, last_name, email)
	)

	id = cur.fetchone()[0]
	return id


def add_phone(cur, client_id: int, phone: str) -> int:
	""" Добавляет номер телефона клиента в таблицу phone_number.

	Входные параметры:
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor
	client_id - целочисленный id клиента
	phone - номер телефона в строковом формате

	Возвращаемое значение:
	В случае успешного добавления строки в таблицу phone_number будет возвращен целочисленный id нового телефонного номера.

	"""

	cur.execute("""
		INSERT INTO phone_number(client_id, number)
		VALUES (%s, %s)
		RETURNING id;
	""", (client_id, phone)
	)

	id = cur.fetchone()[0]
	return id


def update_client(cur, id: int, data: dict) -> int:
	""" Обновляет данные о клиенте в таблице client.

	Входные параметры:
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor
	id - целочисленный id клиента
	data - словарь, ключи которого являются названиями полей, а значения - их новыми данными
	В словаре могут содержаться следующие данные для обновления:
		first_name - новое имя клиента. Не допускается длина более 50 (однобайтовых) символов
		last_name - новая фамилия клиента. Не допускается длина более 50 (однобайтовых) символов
		email - новая электронная почта клиента

	Возвращаемое значение:
	В случае успешного обновления данных в таблице client будет возвращен целочисленный id клиента.

	"""

	cur.execute("""
		SELECT first_name, last_name, email FROM client
		 WHERE id = %s;
	""", (id,)
	)

	old_data = zip(('first_name', 'last_name', 'email'), *cur.fetchall())
	new_data = []
	for key, value in old_data:
		data.setdefault(key, value)
		new_data.append(data[key])

	cur.execute("""
		UPDATE client
		   SET first_name = %s,
		   	   last_name = %s,
		   	   email = %s
		 WHERE id = %s
		RETURNING id;
	""", (*data.values(), id)
	)

	id = cur.fetchone()[0]
	return id


def delete_phone(cur, client_id: int, phone: str) -> int:
	""" Удаляет телефонный номер клиента из таблицы phone_number.

	Входные параметры:
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor
	client_id - целочисленный id клиента
	phone - номер телефона, который нужно удалить, в строковом формате

	Возвращаемое значение:
	В случае успешного удаления будет возвращен целочисленный id удаленной записи.

	"""

	cur.execute("""
		DELETE FROM phone_number
		 WHERE client_id = %s AND number = %s
		RETURNING id;
	""", (client_id, phone)
	)

	id = cur.fetchone()[0]
	return id


def delete_client(cur, id: int) -> int:
	""" Удаляет запись о клиенте из таблицы client.
	    Перед удалением записи о клиенте, удаляет связанные с клиентом номера из таблицы phone_number.

	Входные параметры:
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor
	id - целочисленный id клиента

	Возвращаемое значение:
	В случае успешного удаления будет возвращен целочисленный id удаленной записи.

	"""

	cur.execute("""
		DELETE FROM phone_number
		 WHERE client_id = %s;
	""", (id,)
	)

	cur.execute("""
		DELETE FROM client
		 WHERE id = %s
		RETURNING id;
	""", (id,)
	)

	id = cur.fetchone()[0]
	return id


def find_client(cur, request: dict) -> list:
	""" Находит клиента в базе по его данным.

	Входные параметры:
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor
	request - словарь, в котором ключи - названия полей, по которым производится поиск
	В словаре могут содержаться следующие поля для поиска:
		first_name - имя клиента
		last_name - фамилия клиента
		email - электронная почта клиента
		number - телефонный номер клиента

	Возвращаемое значение:
	Список id клиентов, которые подходят под условия поиска

	"""

	id_sets = []

	for key, value in request.items():
		cur.execute("""
			SELECT DISTINCT c.id
			  FROM client c
			       LEFT JOIN phone_number p
			       ON p.client_id = c.id
			 WHERE %s ~* %s;
		""", (AsIs(key), value)
		)

		id_sets.append(set(id[0] for id in cur.fetchall()))

	id_list = list(set.intersection(*id_sets))
	
	return id_list 


def print_table(cur, table: str) -> None:
	""" Выводит в консоль все данные из таблицы table (список кортежей - строк таблицы).

	Входные параметры:
	cur - курсор подключения к базе данных, объект типа psycopg2.extensions.cursor
	table - название таблицы

	"""

	cur.execute("""
		SELECT * FROM %s;
	""", (AsIs(table),)
	)

	print(cur.fetchall())

if __name__ == '__main__':
	
	with psycopg2.connect(database='netology_db', user='postgres', password='123', host='localhost') as conn:
		with conn.cursor() as cur:
			
			cur.execute("""
				DROP TABLE phone_number;
				DROP TABLE client;
			""")

			create_database(cur)
			conn.commit()
			
			id1 = add_client(cur, 'Petya', 'Petrov', 'petrusha@inbox.com')
			id2 = add_client(cur, 'Vasya', 'Vasilyev', 'vasyandr@inbox.com')

			add_phone(cur, id1, '123')
			add_phone(cur, id1, '321')
			add_phone(cur, id2, '6543')
			add_phone(cur, id2, '456')

			print_table(cur, 'client')
			print_table(cur, 'phone_number')
			new_data = {'first_name': 'Petrusha', 'last_name': 'Petroff'}
			update_client(cur, id1, new_data)
			print('\nafter update client\n')
			print_table(cur, 'client')

			delete_phone(cur, id2, '456')
			print('\nafter phone delete\n')
			print_table(cur, 'phone_number')
			
			print("\nsearch for 'as'\n")
			search_dict1 = {'first_name': 'as'}
			print(find_client(cur, search_dict1))

			print("\nsearch for '3'\n")
			search_dict2 = {'number': '3'}
			print(find_client(cur, search_dict2))

			delete_client(cur, id1)
			delete_client(cur, id2)
			print('\nempty client table\n')
			print_table(cur, 'client')
			print('\nempty phone table\n')
			print_table(cur, 'phone_number')



		