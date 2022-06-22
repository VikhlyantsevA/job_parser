import scrapy
from scrapy.http import HtmlResponse
from urllib.parse import urlencode


from jobparser.items import JobparserItem


class HhSpider(scrapy.Spider):
    name = 'hh'
    allowed_domains = ['hh.ru']
    base_url = 'https://izhevsk.hh.ru/search/vacancy'
    start_urls = []
    for area_code in range(1, 3):
        search_params = [
            ('area', area_code),
            ('search_field', 'name'),
            ('search_field', 'company_name'),
            ('search_field', 'description'),
            ('text', 'аналитик'),
            ('items_on_page', 20),
            ('no_magic', 'true'),
            ('L_save_area', 'true')
        ]
        url = f"{base_url}?{urlencode(search_params)}"
        start_urls.append(url)

    def parse(self, response: HtmlResponse):
        next_page = response.xpath("//a[@data-qa='pager-next']/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

        links = response.xpath("//a[@data-qa='vacancy-serp__vacancy-title']/@href").getall()
        for link in links:
            yield response.follow(link, callback=self.vacancy_parse)

    def vacancy_parse(self, response: HtmlResponse):
        name = response.xpath("//div[@class='bloko-columns-row']//h1[@data-qa='vacancy-title']/text()").get()
        salary = response.xpath("//div[@class='bloko-columns-row']//div[@data-qa='vacancy-salary']//text()").getall()
        published_at = response.xpath("//div[@class='bloko-columns-row']//p[contains(@class, 'vacancy-creation-time')]//text()").getall()
        url = response.url
        yield JobparserItem(name=name, salary=salary, published_at=published_at, url=url)
