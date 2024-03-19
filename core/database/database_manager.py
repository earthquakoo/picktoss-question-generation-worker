import pymysql

class DatabaseManager:
    def __init__(self, host, user, password, db, charset='utf8'):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.db,
            charset=self.charset
        )
        self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)

    def execute_query(self, query, params=None):
        if not self.connection or not self.cursor:
            self.connect()
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            print("Error executing query:", e)
            return None

    def commit(self):
        if self.connection:
            self.connection.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()