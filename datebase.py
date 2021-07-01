import asyncio
import aiomysql
import json

from config import HOST, PORT, USER, PASSWORD, DATABASE_NAME


async def main() -> None:
    connection = await create_connection(loop)
    result = await get_need_update_domains(360, connection)
    connection.close()


async def get_all_domains_in_db(connection):
    data = "SELECT domain FROM domains"
    return [domain[0] for domain in await request(data, connection)]


async def write_mx_list_domains(id_and_records: dict[str, list[int,str]], connection):
    cursor = await get_cursor(connection)
    try:
        query = """
            UPDATE domains SET mx_records =%s, last_update = NOW()
            WHERE id = %s
        """
        values = [(json.dumps(records),id) for id, records in id_and_records.items()]
        await cursor.executemany(query, values)
        await connection.commit()
    finally:
        await cursor.close()


async def get_need_update_domains(delta_time_sec: int, connection) -> list[str]:
    data = f"""
        SELECT domain, id FROM domains
        WHERE unix_timestamp(last_update)<unix_timestamp(NOW())-{delta_time_sec}"""
    dict_need_update = {}
    for response in await request(data, connection):
        dict_need_update.update({response[0]: response[1]})
    return dict_need_update


async def insert_domains(domains: list[str], connection):
    cursor = await get_cursor(connection)
    try:
        query = "INSERT INTO domains (domain) VALUES (%s)"
        await cursor.executemany(query, domains)
        response = await cursor.fetchall()
        await connection.commit()
    finally:
        await cursor.close()
    return [*response]


async def re_create_tables(connection) -> None:
    try: await request('DROP TABLE `domains`', connection)
    except: pass
    create_domains_table = """CREATE TABLE domains (
          id int(11) NOT NULL AUTO_INCREMENT,
          domain text(50) NOT NULL,
          last_update timestamp DEFAULT from_unixtime(1),
          mx_records json DEFAULT NULL,
          PRIMARY KEY (id))"""
    await request(create_domains_table, connection)


async def request(data: str, connection: aiomysql.Connection):
    cursor = await get_cursor(connection)
    try:
        await cursor.execute(data)
        response = await cursor.fetchall()
        await connection.commit()
    finally:
        await cursor.close()
    return [*response]


async def create_connection(loop) -> aiomysql.Connection:
    connection = await aiomysql.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD,
        db=DATABASE_NAME, loop=loop
    )
    return connection


async def get_cursor(connection: aiomysql.Connection) -> aiomysql.Cursor:
    return await connection.cursor()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt: pass
    print('Exit program')
