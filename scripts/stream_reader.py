import psycopg2
import psycopg2.errorcodes

from postgresql_connector import PostgresqlConnector


class StreamReader(object):
    def __init__(
        self,
        pg_info,
        slot_name='pg_slot',
        upto_nchanges=1,
        tables=[]
    ):
        super(StreamReader, self).__init__()
        self.pg_info = pg_info
        self.slot_name = slot_name
        self.upto_nchanges = upto_nchanges
        self.tables = tables

    def init_pg_stuffs(self):
        pg_info = self.pg_info
        pg_connector = PostgresqlConnector(
            pg_info['host'],
            pg_info['port'],
            pg_info['database'],
            pg_info['user'],
            pg_info['password']
        ).connect()
        pg_cursor = pg_connector.cursor()
        return pg_connector, pg_cursor

    def create_logical_replication_slot_if_not_existed(
        self,
        pg_cursor,
        pg_connector,
        logger
    ):
        try:
            slot_name = self.slot_name
            pg_cursor.execute(
                """
                SELECT *
                FROM pg_create_logical_replication_slot('{slot_name}', 'test_decoding');
                """
                .format(slot_name=slot_name)
            )
            slot_info = pg_cursor.fetchall()
            logger.info('Created slot with info: ' + str(slot_info))

        except psycopg2.ProgrammingError as p:
            pg_connector.commit()
            if p.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
                logger.error(str(p))
                raise
            else:
                logger.warning('Slot {slot_name} is already present.'.format(
                    slot_name=slot_name))

    def destroy_logical_replication_slot_if_existed(
        self,
        pg_cursor,
        pg_connector,
        logger
    ):
        try:
            slot_name = self.slot_name
            pg_cursor.execute(
                """
                SELECT pg_drop_replication_slot('{slot_name}');
                """
                .format(slot_name=slot_name)
            )
            destroy_info = pg_cursor.fetchall()
            logger.info('Slot destroying Info: ' + str(destroy_info))

        except psycopg2.ProgrammingError as p:
            if p.pgcode != psycopg2.errorcodes.UNDEFINED_OBJECT:
                logger.error(str(p))
                raise
            else:
                logger.info('Slot {slot_name} was not found.'.format(
                    slot_name=slot_name))

    def retrieve_changes(self, pg_cursor):
        pg_cursor.execute(
            """
            SELECT *
            FROM pg_logical_slot_peek_changes('{slot_name}', NULL, {upto_nchanges});
            """
            .format(
                upto_nchanges=self.upto_nchanges,
                slot_name=self.slot_name
            )
        )
        return pg_cursor.fetchall()

    def delete_changes_after_comsumed(self, pg_cursor):
        pg_cursor.execute(
            """
            SELECT *
            FROM pg_logical_slot_get_changes('{slot_name}', NULL, {upto_nchanges});
            """
            .format(
                upto_nchanges=self.upto_nchanges,
                slot_name=self.slot_name
            )
        )
        return pg_cursor.fetchall()
