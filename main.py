import asyncio
from async_dns.core import types, Address, Record
from async_dns.resolver import DNSClient
from time import time
from typing import Union

from config import FILENAME, LIST_DNS_SERVERS, NEED_UPDATE_SECOUNDS, CHECK_TIME

from datebase import (
            create_connection,
            insert_domains,
            get_need_update_domains,
            get_all_domains_in_db,
            write_mx_list_domains,
            re_create_tables)



async def main() -> None:
    try:
        print('Starting program')
        connection = await create_connection(loop)
        # await re_create_tables(connection) #пересоздает таблицу!!!!
        # await read_emails_and_write_domains_to_db(FILENAME, connection)

        print('Start checking cycle')
        while True:
            need_update_domains = await get_need_update_domains(NEED_UPDATE_SECOUNDS, connection)#не обновлялось час
            if need_update_domains:
                print(f"Length list domains need update records: {len(need_update_domains)}")
                await checker(need_update_domains, LIST_DNS_SERVERS, connection)

            connection.close()
            await asyncio.sleep(CHECK_TIME)
            connection = await create_connection(loop)
    finally:
        connection.close()


async def read_emails_and_write_domains_to_db(filename: str, connection):
    emails = get_emails_in_file(filename)
    domains = convert_emails_to_domains(emails)
    unique_domains = get_unique_domains(domains)
    print(f"Length unique domains in file: {len(unique_domains)}")
    db_domains = await get_all_domains_in_db(connection)
    print(f"Length unique domains in db: {len({*db_domains})}")
    missing_domains = find_missing_values(db_domains, unique_domains)
    if missing_domains:
        await insert_domains(missing_domains, connection)
        print(f"Writed {len(missing_domains)} unique domains in file to db")
    else:
        print(f"All domains from file in db")


async def checker(
        dict_domains: dict[str, int],
        list_dns_servers: list[str],
        db_connection,
        length_chunk: int = 500) -> dict[str, list[tuple[int, str]]]:
    list_domains = [*dict_domains.keys()]
    checked_dict = {}
    dns_server = list_dns_servers.pop(0)
    start_time = time()
    while list_domains:
        current_size_chunk = len(list_domains) if len(list_domains) < length_chunk \
            else length_chunk
        chunk = [list_domains.pop(0) for _ in range(current_size_chunk)]
        result = await get_mx_in_domains(chunk, dns_server)
        checked_dict_current = result.get('checked')
        database_dict_update = {}
        for key, value in checked_dict_current.items():
            id = dict_domains.pop(key)
            database_dict_update.update({id:value})
        await write_mx_list_domains(database_dict_update, db_connection)
        checked_dict.update(checked_dict_current)
        log = f"Work Time: {round(time() - start_time, 1)}s | "
        log += f"Checked: {len(checked_dict)}. Residue: {len(list_domains)} | "
        log += f"DNS: {dns_server} | "
        log += f"[{len(result.get('checked'))}:{len(result.get('unchecked'))}]"
        list_domains.extend(result.get('unchecked'))
        if result.get('ban') or \
                len(result.get('checked')) < current_size_chunk / 100:
            list_dns_servers.append(dns_server)
            dns_server = list_dns_servers.pop(0)
            log += " | DNS UPDATE"
        print(log)
    print(f'Checker end! Length checked dict: {len(checked_dict)}')
    return checked_dict


async def get_mx(domain: str, client: DNSClient, dns_server: str = '1.1.1.1'
                 ) -> tuple[str, Union[str, list[tuple[int, str]]]]:
    address = Address.parse(dns_server)
    result = []
    try:
        raw_list_records = await client.query(domain, types.MX, address)
        for record in raw_list_records: result.append([*record.data.data])
    except asyncio.exceptions.TimeoutError:
        result = 'timeout'
    except asyncio.exceptions.CancelledError:
        result = 'ban'
    return (domain, result)


async def get_mx_in_domains(list_domains: list[str], dns_server):
    client = DNSClient(timeout=1)
    tasks = [get_mx(domain, client, dns_server) for domain in list_domains]
    output = {'checked': {}, 'unchecked': [], 'ban': False}
    for task in asyncio.as_completed(tasks):
        domain, result = await task
        if isinstance(result, list):
            output['checked'].update({domain: result})
        else:
            output['unchecked'].append(domain)
            if result == 'ban': output.update(ban=True)
    return output


def get_emails_in_file(filename: str) -> list[str]:
    with open(filename, 'r', encoding='utf-8') as f:
        text_array = f.read()
        return text_array.split('\n')


def convert_emails_to_domains(list_emails: list[str]) -> list[str]:
    domains = []
    for email in list_emails:
        if '@' in email: domains.append(email.split('@')[1])
    return domains


def get_unique_domains(list_domains: list[str]) -> list[str]:
    return [*{*list_domains}]

def find_missing_values(verifiable_list: list, values_list: list) -> list:
    missing_elements = []
    if not values_list: return missing_elements
    if not verifiable_list: return values_list
    verifiable_set = {*verifiable_list}

    for value in values_list:
        if not value in verifiable_set: missing_elements.append(value)
    return missing_elements

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt: pass
    print('Exit program')
