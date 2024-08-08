import redis
import os
import dotenv
import requests
from bs4 import BeautifulSoup
import re
from data.guap_data import guap_tables_urls

dotenv.load_dotenv()

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_DB = os.getenv('REDIS_DB', 0)

# Initialize Redis client
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def parse_guap(url):
    try:
        response = requests.get(url)
        response.encoding = 'utf-8'

        if response.status_code != 200:
            print(f"Failed to retrieve the URL: {url}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')

        # Extract the direction name
        direction_name = None
        direction_pattern = re.compile(r'^\d{2}\.\d{2}\.\d{2}\s".+?"')
        for h3 in soup.find_all('h3'):
            if direction_pattern.match(h3.text.strip()):
                direction_name = h3.text.strip()
                break

        # Extract the update date
        update_date = None
        for p in soup.find_all('p'):
            if 'Дата актуализации' in p.text:
                update_date = p.text.strip().replace('Дата актуализации - ', '')
                break

        if not update_date:
            print(f"No update date found at URL: {url}")
            return None

        if not direction_name:
            print(f"No direction name found at URL: {url}")
            return None

        if not table:
            print(f"Table not found at URL: {url}")
            return None

        headers = [header.text.strip() for header in table.find_all('th')]
        new_headers = {
            'п/п': 'position',
            'СНИЛС/Идентификатор': 'snils',
            'Приоритет': 'priority',
            'Сумма конкурсных баллов': 'total_score',
            'Оригинал документа об образовании': 'original',
            'Дата актуализации': 'update_date',
        }
        data_guap = []
        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            row_data = {new_headers[headers[i]]: cells[i].text.strip() for i in [0, 1, 2, 3, 6]}
            row_data['direction'] = direction_name
            row_data['update_date'] = update_date
            data_guap.append(row_data)

        return data_guap

    except Exception as e:
        print(f"An error occurred while parsing the URL: {url}\nError: {e}")
        return None


def insert_data_into_redis(data):
    for entry in data:
        try:
            key = f"guap:{entry['direction']}:{entry['position']}"
            r.hset(key, mapping=entry)
            print(f"Data inserted into Redis: {entry}")
        except Exception as e:
            print(f"Insert failed for {entry}: {e}")


def insert_guap_data():
    urls = guap_tables_urls
    for url in urls:
        data = parse_guap(url)
        if data:
            insert_data_into_redis(data)


if __name__ == "__main__":
    insert_guap_data()
