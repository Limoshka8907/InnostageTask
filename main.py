# import psycopg2
# from ldap3 import Server, Connection, ALL
# import uuid
#
# class DbManager():
#
#     def __init__(self):
#       # Подключаюсь к sql
#         self.pg_connection = psycopg2.connect(
#             dbname="ToWinServer",
#             user="user1",
#             password="123456sS",
#             host="localhost",
#             port="5432"
#         )
#
#     def fetch_ad_data(self):
#         # Active Directory
#         ldap_server = 'ldap://192.168.56.101'
#         user_dn = 'CN=Администратор,CN=Users,DC=MainForest,DC=com'
#         password = '123456sS'
#
#         server = Server(ldap_server, get_info=ALL)
#         conn = Connection(server, user_dn, password, auto_bind=True)
#         self.conn = conn
#
#         # Запрос всех пользователей
#         conn.search('dc=MainForest,dc=com', '(objectClass=user)',
#                     attributes=['objectGUID', 'userPrincipalName', 'sAMAccountName', 'givenName', 'sn', 'middleName',
#                                 'lastLogon', 'memberOf'])
#
#         return conn.entries
#
#
#     def update_database(self, entries):
#         with self.pg_connection:
#             with self.pg_connection.cursor() as cursor:
#                 # Обновляем таблицу Users
#                 for entry in entries:
#                     if entry.objectGUID and entry.objectGUID.value:
#                         user_guid_value = entry.objectGUID.value
#                         try:
#                             user_guid = uuid.UUID(user_guid_value)
#                         except ValueError:
#                             raise ValueError(f"Invalid user objectGUID format: {user_guid_value}")
#                         print(f"Users user - {user_guid} name - {entry.sAMAccountName}")
#                         cursor.execute("""
#                             INSERT INTO Users (guid, user_principal_name, sam_account_name, first_name, last_name, middle_name, last_login)
#                             VALUES (%s, %s, %s, %s, %s, %s, %s)
#                             ON CONFLICT (guid) DO UPDATE SET
#                                 user_principal_name = EXCLUDED.user_principal_name,
#                                 sam_account_name = EXCLUDED.sam_account_name,
#                                 first_name = EXCLUDED.first_name,
#                                 last_name = EXCLUDED.last_name,
#                                 middle_name = EXCLUDED.middle_name,
#                                 last_login = EXCLUDED.last_login;
#                         """, (str(user_guid), entry.userPrincipalName.value, entry.sAMAccountName.value,
#                               entry.givenName.value,
#                               entry.sn.value, entry.middleName.value if 'middleName' in entry else None,
#                               entry.lastLogon.value if 'lastLogon' in entry else None))
#
#                 # Обновляем таблицу Groups
#                 self.conn.search('dc=MainForest,dc=com', '(objectClass=group)', attributes=['objectGUID', 'cn', 'name'])
#                 for group in self.conn.entries:
#                     if group.objectGUID and group.objectGUID.value:
#                         group_guid_value = group.objectGUID.value
#                         try:
#                             group_guid = uuid.UUID(group_guid_value)
#                         except ValueError:
#                             raise ValueError(f"Invalid group objectGUID format: {group_guid_value}")
#                         print(f"Groups group_name - {group.name.value} group - {group_guid}")
#
#                         cursor.execute("""
#                             INSERT INTO Groups (guid, cn, name)
#                             VALUES (%s, %s, %s)
#                             ON CONFLICT (guid) DO NOTHING;
#                         """, (str(group_guid), group.cn.value, group.name.value))
#
#                 # Обновляем UsersInGroups
#                 for entry in entries:
#                     if entry.objectGUID and entry.objectGUID.value:
#                         user_guid_value = entry.objectGUID.value
#                         try:
#                             user_guid = uuid.UUID(user_guid_value)
#                         except ValueError:
#                             raise ValueError(f"Invalid user objectGUID format for UsersInGroups: {user_guid_value}")
#
#                             # Получаем GUID пользователя
#                         cursor.execute("SELECT guid FROM Users WHERE guid = %s;", (str(user_guid),))
#                         user_id = cursor.fetchone()
#                         if user_id is None:
#                             continue  # Пропускаем, если пользователь не найден
#                         i = 0
#                         for group_dn in entry.memberOf:
#                             i+=1
#                             # Проверяем, что group_dn является строкой
#                             if isinstance(group_dn, str):
#                                 # Костыль для просмотра группы, не нашёл ничего лучше
#                                 group_name = [part for part in group_dn.split(',') if part.startswith('CN=')][0]
#                                 # Убираем 'CN=' и лишние пробелы
#                                 group_name = group_name.replace('CN=', '').strip()
#                                 cursor.execute("SELECT guid FROM Groups WHERE cn = %s;", (group_name,))
#                                 group_id_row = cursor.fetchone()
#
#                                 if group_id_row:
#                                     group_guid_value = group_id_row[0]
#                                     try:
#                                         group_guid = uuid.UUID(group_guid_value)
#                                     except ValueError:
#                                         raise ValueError(f"Invalid group GUID format: {group_guid_value}")
#                                     print(f"#{i} UsersInGroups user - {user_guid} group - {group_guid}")
#                                         # Вставляем данные в UsersInGroups
#                                     cursor.execute("""
#                                         INSERT INTO UsersInGroups (user_guid, group_guid)
#                                         VALUES (%s, %s)
#                                         ON CONFLICT (user_guid, group_guid) DO NOTHING;
#                                     """, (str(user_guid), str(group_guid)))
#
# if __name__ == "__main__":
#     db_m = DbManager()
#     entries = db_m.fetch_ad_data()
#     db_m.update_database(entries)


#Version 1.2
import psycopg2
from ldap3 import Server, Connection, ALL
import uuid
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DbManager:
    def __init__(self):
        self.pg_connection = self._connect_to_postgres()
        self.ad_connection = self._connect_to_active_directory()

    def _connect_to_postgres(self):
        try:
            return psycopg2.connect(
                dbname="ToWinServer",
                user="user1",
                password="123456sS",
                host="localhost",
                port="5432"
            )
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _connect_to_active_directory(self):
        try:
            ldap_server = 'ldap://192.168.56.101'
            user_dn = 'CN=Администратор,CN=Users,DC=MainForest,DC=com'
            password = '123456sS'
            server = Server(ldap_server, get_info=ALL)
            return Connection(server, user_dn, password, auto_bind=True)
        except Exception as e:
            logging.error(f"Failed to connect to Active Directory: {e}")
            raise

    def fetch_ad_data(self):
        try:
            self.ad_connection.search('dc=MainForest,dc=com', '(objectClass=user)',
                        attributes=['objectGUID', 'userPrincipalName', 'sAMAccountName', 'givenName', 'sn', 'middleName',
                                    'lastLogon', 'memberOf'])
            return self.ad_connection.entries
        except Exception as e:
            logging.error(f"Failed to fetch AD data: {e}")
            raise

    def update_database(self, entries):
        try:
            with self.pg_connection:
                with self.pg_connection.cursor() as cursor:
                    self._update_users(cursor, entries)
                    self._remove_absent_users(cursor, entries) #Новый метод, для удаления
                    self._update_groups(cursor)
                    self._update_users_in_groups(cursor, entries)

            logging.info("Database update completed successfully")
        except Exception as e:
            logging.error(f"Failed to update database: {e}")
            raise

    def _update_users(self, cursor, entries):
        for entry in entries:
            if entry.objectGUID and entry.objectGUID.value:
                user_guid = self._parse_guid(entry.objectGUID.value, "user")
                logging.info(f"Updating user - {user_guid}, name - {entry.sAMAccountName}")
                cursor.execute("""
                    INSERT INTO Users (guid, user_principal_name, sam_account_name, first_name, last_name, middle_name, last_login)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (guid) DO UPDATE SET
                        user_principal_name = EXCLUDED.user_principal_name,
                        sam_account_name = EXCLUDED.sam_account_name,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        middle_name = EXCLUDED.middle_name,
                        last_login = EXCLUDED.last_login;
                """, (str(user_guid), entry.userPrincipalName.value, entry.sAMAccountName.value,
                      entry.givenName.value, entry.sn.value,
                      entry.middleName.value if 'middleName' in entry else None,
                      entry.lastLogon.value if 'lastLogon' in entry else None))

    def _update_groups(self, cursor):
        self.ad_connection.search('dc=MainForest,dc=com', '(objectClass=group)', attributes=['objectGUID', 'cn', 'name'])
        for group in self.ad_connection.entries:
            if group.objectGUID and group.objectGUID.value:
                group_guid = self._parse_guid(group.objectGUID.value, "group")
                logging.info(f"Updating group - {group.name.value}, GUID - {group_guid}")
                cursor.execute("""
                    INSERT INTO Groups (guid, cn, name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (guid) DO NOTHING;
                """, (str(group_guid), group.cn.value, group.name.value))

    def _update_users_in_groups(self, cursor, entries):
        for entry in entries:
            if entry.objectGUID and entry.objectGUID.value:
                user_guid = self._parse_guid(entry.objectGUID.value, "user")
                cursor.execute("SELECT guid FROM Users WHERE guid = %s;", (str(user_guid),))
                if cursor.fetchone() is None:
                    continue

                for i, group_dn in enumerate(entry.memberOf, 1):
                    if isinstance(group_dn, str):
                        group_name = group_dn.split(',')[0].replace('CN=', '').strip()
                        cursor.execute("SELECT guid FROM Groups WHERE cn = %s;", (group_name,))
                        group_id_row = cursor.fetchone()
                        if group_id_row:
                            group_guid = self._parse_guid(group_id_row[0], "group")
                            logging.info(f"Updating UsersInGroups - User: {user_guid}, Group: {group_guid}")
                            cursor.execute("""
                                INSERT INTO UsersInGroups (user_guid, group_guid)
                                VALUES (%s, %s)
                                ON CONFLICT (user_guid, group_guid) DO NOTHING;
                            """, (str(user_guid), str(group_guid)))

    def _remove_absent_users(self, cursor, entries):
        # Получаем GUID всех пользователей, которые будут добавлены или обновлены
        existing_guids = {str(self._parse_guid(entry.objectGUID.value, "user")) for entry in entries if
                          entry.objectGUID and entry.objectGUID.value}

        # Получаем GUID всех пользователей в базе данных
        cursor.execute("SELECT guid FROM Users;")
        db_guids = {row[0] for row in cursor.fetchall()}

        # Логируем полученные GUID
        #TODO: Проверь логи
        logging.info(f"Existing GUIDs from AD: {existing_guids}")
        logging.info(f"GUIDs in database: {db_guids}")

        # Находим пользователей, которые есть в базе данных, но отсутствуют в новых данных
        users_to_delete = db_guids - existing_guids

        # Логируем пользователей, которые будут удалены
        logging.info(f"Users to delete: {users_to_delete}")

        for user_guid in users_to_delete:
            logging.info(f"Deleting user - {user_guid}")
            # Сначала удаляем связи пользователя с группами
            cursor.execute("DELETE FROM UsersInGroups WHERE user_guid = %s;", (str(user_guid),))
            # Затем удаляем самого пользователя
            cursor.execute("DELETE FROM Users WHERE guid = %s;", (str(user_guid),))


    @staticmethod
    def _parse_guid(guid_value, entity_type):
        try:
            return uuid.UUID(guid_value)
        except ValueError:
            raise ValueError(f"Invalid {entity_type} objectGUID format: {guid_value}")

if __name__ == "__main__":
    try:
        db_manager = DbManager()
        ad_entries = db_manager.fetch_ad_data()
        db_manager.update_database(ad_entries)
    except Exception as e:
        logging.error(f"An error occurred: {e}")