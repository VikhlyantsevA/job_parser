import time

import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from scrapy.http import HtmlResponse
from lxml import html
from urllib.parse import urljoin, urlparse
import os.path

from jobparser.items import JobparserItem
from jobparser.settings import PROJECT_ROOT


class SuperjobSpider(scrapy.Spider):
    name = 'superjob'
    allowed_domains = ['superjob.ru']
    base_url = 'http://superjob.ru/'
    start_urls = []

    options = Options()
    options.add_argument("start-maximized")
    service = Service(os.path.join(PROJECT_ROOT, "drivers", "chromedriver.exe"))

    regions = ["Санкт-Петербург", "Москва"]
    vacancies = ["аналитик"]

    # Получаем ссылки на стартовые страницы с результатами поискового запроса
    with webdriver.Chrome(service=service, options=options) as driver:
        driver.implicitly_wait(1)
        wait = WebDriverWait(driver, 15)

        for vacancy in vacancies:
            for region in regions:
                driver.get(base_url)

                # Согласие на использование кук
                try:
                    driver.find_element(By.XPATH, '//button[contains(@class, "f-test-button-Soglasen")]').click()
                except NoSuchElementException:
                    pass

                # Выбор региона
                wait.until(EC.element_to_be_clickable((By.XPATH, '//form[@action="/vacancy/search/"]//span[@role="button"]'))) \
                    .click()
                # Очистка списка регионов
                wait.until(EC.element_to_be_clickable((By.XPATH, '//button[contains(@class, "f-test-button-Ochistit")]'))) \
                    .click()
                # Фильтрация регионов
                wait.until(EC.presence_of_element_located((By.XPATH, '//input[@name="geo"]'))).send_keys(region)
                # Выбор нужного региона из списка
                wait.until(EC.element_to_be_clickable((By.XPATH, f'//label//span[contains(text(), "{region}")]'))) \
                    .click()

                wait.until(EC.element_to_be_clickable((By.XPATH, '//button[contains(@class, "f-test-button-Primenit")]'))) \
                    .click()
                geo_base_url = driver.current_url

                # Ввод вакансии
                wait.until(EC.invisibility_of_element_located((By.ID, 'headerLocationGeoCurtain')))
                driver.find_element(By.XPATH, '//input[@name="keywords"]').send_keys(vacancy, Keys.ENTER)

                wait.until_not(EC.url_to_be(geo_base_url))
                time.sleep(2)
                start_urls.append(driver.current_url)

    def parse(self, response: HtmlResponse):
        next_page = response.xpath('//a[contains(@class, "f-test-link-Dalshe")]').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

        job_dom = html.fromstring(response.text)
        job_blocks = job_dom.xpath('//div[contains(@class, "f-test-vacancy-item")]')
        for block in job_blocks:
            name = ''.join(block.xpath('.//a[@target="_blank"]//text()'))
            salary = block.xpath('.//span[contains(@class, "f-test-text-company-item-salary")]//text()')
            published_at = block.xpath('.//span[contains(@class, "f-test-text-company-item-location")]//text()')[0]
            url_raw = block.xpath('.//a[@target="_blank"]/@href')[0]
            start_url_parsed = urlparse(response.url)
            start_url_base = f'{start_url_parsed.scheme}://{start_url_parsed.hostname}/'
            url = urljoin(start_url_base, url_raw) if url_raw.startswith('/') else url_raw

            yield JobparserItem(name=name, salary=salary, published_at=published_at, url=url)
