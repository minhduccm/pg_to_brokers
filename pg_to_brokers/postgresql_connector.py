import psycopg2


class PostgresqlConnector(object):
    def __init__(self, host, port, database, user, password):
        super(PostgresqlConnector, self).__init__()
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        conn_info_with_psycopg2 = 'dbname={dbname} port={port} user={user} password={password} host={host}' \
            .format(
                dbname=self.database,
                port=self.port,
                user=self.user,
                password=self.password,
                host=self.host
            )
        return psycopg2.connect(conn_info_with_psycopg2)
