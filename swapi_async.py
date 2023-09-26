import aiohttp
import asyncio
import datetime

from aiohttp import ClientSession
from more_itertools import chunked
from models import SwapiPeople, Base, engine, Session

CHUNK_SIZE = 10


async def get_people(people_id, client):
    response = await client.get(f'https://swapi.dev/api/people/{people_id}')
    json_data = await response.json()
    return json_data


async def get_len(client):
    response = await client.get(f'https://swapi.dev/api/people/')
    info = await response.json()
    person_quantity = info['count']
    return person_quantity


async def get_list_data(list_data: list, list_field: str):
    item_list = []
    if type(list_data) != list:
        list_data = []
    for data in list_data:
        async with ClientSession() as session:
            async with session.get(data) as response:
                json_data = await response.json()
                if json_data.get(list_field):
                    item_list.append(json_data.get(list_field))
    received_list_data = ', '.join(item_list)
    return received_list_data


async def insert_to_db(people_data_json):
    async with Session() as session:
        people_data_list = []
        for people_data in people_data_json:
            try:
                people_data_dict = {
                               'birth_year': people_data['birth_year'],
                               'eye_color':  people_data['eye_color'],
                               'films':      await get_list_data(people_data.get('films'), 'title'),
                               'gender':     people_data['gender'],
                               'hair_color': people_data['hair_color'],
                               'height':     people_data['height'],
                               'homeworld':  people_data['homeworld'],
                               'mass':       people_data['mass'],
                               'name':       people_data['name'],
                               'skin_color': people_data['skin_color'],
                               'species':    await get_list_data(people_data.get('species'), 'name'),
                               'starships':  await get_list_data(people_data.get('starships'), 'name'),
                               'vehicles':   await get_list_data(people_data.get('vehicles'), 'name')
                               }
            except KeyError:
                pass
            people_data_list.append(people_data_dict)
        all_swapi_people_data = [SwapiPeople(**item) for item in people_data_list]
        session.add_all(all_swapi_people_data)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    tasks = []
    async with aiohttp.ClientSession() as client:
        people_len = await get_len(client)
        for ids_chunk in chunked(range(1, people_len + 1), CHUNK_SIZE):
            print(ids_chunk)
            people_coros = []
            for people_id in ids_chunk:
                people_coro = get_people(people_id, client)
                people_coros.append(people_coro)
            result = await asyncio.gather(*people_coros)

            insert_to_db_coro = insert_to_db(result)
            insert_to_db_task = asyncio.create_task(insert_to_db_coro)
            tasks.append(insert_to_db_task)
    tasks = asyncio.all_tasks() - {asyncio.current_task(), }
    for task in tasks:
        await task


if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
