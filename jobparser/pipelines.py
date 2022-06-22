# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo.errors import DuplicateKeyError
from pymongo import MongoClient
from datetime import datetime
import unicodedata
import dateparser
import re

from my_lib.utils import hash_struct


class JobparserPipeline:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongobase = client.vacancy


    def process_item(self, item, spider):
        for k, v in self.process_salary(item['salary']).items():
            if k not in ['min_salary', 'max_salary', 'currency'] and not v:
                continue
            item[k] = v

        item['published_at'] = self.process_date(item['published_at'], spider.name)

        del item['salary']

        item['_id'] = hash_struct(dict(item))
        collection = self.mongobase[spider.name]
        try:
            collection.insert_one(item)
        except DuplicateKeyError:
            print(f"Document with key {item['_id']} already exists.")
        return item

    def process_salary(self, salary):
        salary = unicodedata.normalize('NFKC', ' '.join(salary)).strip()
        salary_info = re.sub(r'(\d)\s(\d)', r'\1\2', re.sub(r'\s+', ' ', salary))

        pattern_1 = re.compile(r'(?:з/п\sне\sуказана)|(?:По\sдоговорённости)')

        pattern_2 = re.compile('(?:(?P<min_salary>\d+)\s?[—–-]\s?)?'
                               '(?P<max_salary>\d+)\s'
                               '(?P<currency>[a-zа-яё]+)\.?\s?'
                               '(?:(?P<net>на\sруки)|(?P<gross>до\sвычета\sналогов))?\s?'
                               '(?:/\s?(?P<rate_type>\w.*))?',
                               re.I | re.X)

        pattern_3 = re.compile('(?:от\s(?P<min_salary>\d+)\s)?'
                               '(?:до\s(?P<max_salary>\d+)\s)?'
                               '(?P<currency>[a-zа-яё]+)\.?\s?'
                               '(?:(?P<net>на\sруки)|(?P<gross>до\sвычета\sналогов))?\s?'
                               '(?:/\s?(?P<rate_type>\w.*))?',
                               re.I | re.X)


        patterns = [pattern_1, pattern_2, pattern_3]
        for i, pattern in enumerate(patterns):
            match = pattern.fullmatch(salary_info)
            if match:
                salary_info = match.groupdict()
                break
            if i == len(patterns) - 1:
                raise Exception(f"Unknown pattern.\nSalary info:{salary_info}")

        res = {
            'min_salary': salary_info.get('min_salary'),
            'max_salary': salary_info.get('max_salary'),
            'currency': salary_info.get('currency'),
            'tax': 'net' if salary_info.get('net') else 'gross' if salary_info.get('gross') else None,
            'rate_type': salary_info.get('rate_type')
        }
        return res

    def process_date(self, published_at, spider_name):
        if spider_name == 'hh':
            published_at = unicodedata.normalize('NFKC', ''.join(published_at)).strip()
            pattern = re.compile(r'[a-zа-яё\s]*(?P<published_at_str>\d{1,2}\s[a-zа-яё]+\s\d{4}).*', re.I)
            day_, month_, year_ = pattern.fullmatch(published_at)['published_at_str'].split(' ')
            published_at = f'{int(day_):02d} {month_.lower()} {year_}'
        date_formats = ['%d %B %Y %H:%M', '%d %B', '%d %B %Y']
        published_at = dateparser.parse(published_at, date_formats=date_formats, languages=['ru'])
        res = datetime.strftime(published_at, '%Y-%m-%d %H:%M')
        return res