import time
import os
import aiopg

from services.utils import DBConfiguration

class PostgresUtils(object):
    @staticmethod
    async def is_present(table_name):
        with (await AsyncPostgresDB.get_instance().pool.cursor()) as cur:
            await cur.execute(
                "select * from information_schema.tables where table_name=%s",
                (table_name,),
            )
            return bool(cur.rowcount)


class AsyncPostgresDB(object):
    connection = None
    __instance = None

    pool = None

    @staticmethod
    def get_instance():
        if AsyncPostgresDB.__instance is None:
            AsyncPostgresDB()
        return AsyncPostgresDB.__instance

    def __init__(self):
        if self.__instance is not None:
            return

        AsyncPostgresDB.__instance = self

    async def _init(self, db_conf: DBConfiguration):
        # todo make poolsize min and max configurable as well as timeout
        # todo add retry and better error message
        retries = 3
        for i in range(retries):
            try:
                self.pool = await aiopg.create_pool(db_conf.dsn, timeout=db_conf.timeout)
            except Exception as e:
                print("printing connection exception: " + str(e))
                if retries - i < 1:
                    raise e
                time.sleep(1)
                continue
