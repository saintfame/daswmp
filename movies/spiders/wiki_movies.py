import scrapy
import logging
import csv
import time

logging.getLogger('scrapy').propagate = False
start_time = time.time()
count = 0
timecount = 0
start_time = time.time()
all_data = [['Название', 'Жанры', 'Режиссёры', 'Страны', 'Год', 'url']]
print('Парсер запущен, принты отключены для увеличения скорости')

class WikiMoviesSpider(scrapy.Spider):
    name = "wiki-movies"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"]

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("ROBOTSTXT_OBEY", "False", priority="spider")
        settings.set("CONCURRENT_REQUESTS", "32", priority="spider")
        settings.set("CONCURRENT_REQUESTS_PER_DOMAIN", "32", priority="spider")

    def parse(self, response):
        next_page_link = response.css("#mw-pages>a::attr('href')").extract()
        if len(next_page_link) == 1:
            next_page = 'https://ru.wikipedia.org'+next_page_link[0]
            yield scrapy.Request(next_page, callback = self.parse)
        else:
            next_page = 'https://ru.wikipedia.org'+next_page_link[1]
            yield scrapy.Request(next_page, callback = self.parse)
        links = response.css(".mw-category-columns a::attr(href)").extract()
        for link in links:
            page = 'https://ru.wikipedia.org'+link
            yield scrapy.Request(page, callback=self.parse_movie)
        
    def parse_movie(self, response):
        global count
        global all_data
        count += 1

        name = None
        genre = ''
        director = ''
        country = ''
        genres = []
        directors = []
        countries = []
        not_series = True

        pre_name = response.css('.mw-page-title-main::text').extract_first()
        url = response.url

        if pre_name is not None:
            if ' (серия фильмов)' in pre_name:
                not_series = False
        if not_series:
            for test1 in response.css('.infobox[data-name*="ильм"]'):

                for test in test1.css('.infobox-above'):
                    name = test.css("span::text").extract_first()
                    if name is None or name == '':
                        name = test.css("th i::text").extract_first()
                    if name is None or name == '':
                        name = test.css("::text").extract_first()

                for test in test1.css('td.plainlist [data-wikidata-property-id="P136"]'):
                    genres = genres + test.css('a::text').extract()
                    genres = genres + test.css('::text').extract()
                    genres = genres + test.css('a span::text').extract()
                    genres = genres + test.css('span::text').extract()

                for test in test1.css('td.plainlist [data-wikidata-property-id="P57"]'):
                    directors = directors + test.css('a:not(.extiw)[title]::text').extract()
                    directors = list(set(directors))
                    directors = directors + test.css('a span::text').extract()
                    directors = list(set(directors))
                    directors = directors + test.css('::text').extract()
                    directors = list(set(directors))
                    directors = directors + test.css('a[title]::text').extract()
                    directors = [x.replace("\n", "").replace(",", "").replace("?", "").replace("!", "").replace("(рус.)", "").replace("(англ.)", "").replace("(фр.)", "").replace("(", "").replace("рус.", "").replace("/", "").replace(")", "").strip() for x in directors if not x.startswith('[')]
                    directors = list(set(directors))

                for test in test1.css('td.plainlist [data-wikidata-property-id="P495"]'):
                    countries = countries + test.css('.country-name a[title]::text').extract()
                    countries = countries + test.css('a[title]::text').extract()
                    countries = countries + test.css('.wrap::text').extract()
                    countries = countries + test.css('::text').extract()

            if len(genres) != 0:
                genres = [x.replace("\n", "").replace(",", "").replace("рус.", "").replace("/", "").replace("англ.", "").strip() for x in genres if not x.startswith('[')]
                genres = set(genres)
                if '' in genres:
                    genres.remove('')
                if '•' in genres:
                    genres.remove('•')
                if '(' in genres:
                    genres.remove('(')
                if ')' in genres:
                    genres.remove(')')
                if '()' in genres:
                    genres.remove('()')
                if '.' in genres:
                    genres.remove('.')
                if ',' in genres:
                    genres.remove(',')
                if ';' in genres:
                    genres.remove(';')
                if '*' in genres:
                    genres.remove('*')
                if '-' in genres:
                    genres.remove('-')
                if None in genres:
                    genres.remove(None)
                genre = ", ".join(str(element) for element in genres)

            if len(directors) != 0:
                if '' in directors:
                    directors.remove('')
                if '/' in directors:
                    directors.remove('/')
                if '-' in directors:
                    directors.remove('-')
                if None in directors:
                    directors.remove(None)
                director = ", ".join(str(element) for element in directors)

            if len(countries) != 0:
                countries = [x.replace("\n", "").replace(",", "").strip() for x in countries if not x.startswith('[')]
                countries = set(countries)
                if '(' in countries:
                    countries.remove('(')
                if ')' in countries:
                    countries.remove(')')
                if '/' in countries:
                    countries.remove('/')
                if '' in countries:
                    countries.remove('')
                if None in countries:
                    countries.remove(None)
                country = ", ".join(str(element) for element in countries)

            year = response.css('.dtstart::text').extract_first()
            if year is None:
                year = response.css('.infobox td.plainlist [data-wikidata-property-id="P577"]  a[title]:last-child::text').extract_first()
            if year is None:
                year = response.css('.infobox td.plainlist [data-wikidata-property-id="P580"]::text').extract_first() 
                if year is not None and year != ' ':
                    year= year.split()[-1]
                if year == ' ':
                    year = None
            if year is None:
                year = response.css('.infobox td.plainlist [data-wikidata-property-id="P580"] a[title].mw-redirect::text').extract_first() 
            if year is None:
                year = response.css('.infobox td.plainlist a[title*=" год"]::text').extract_first()
            if year is None:
                years = response.css('.infobox td.plainlist::text').extract()
                for line in years:
                    if len(line) != 1:
                        year = line.strip()
            if year is None:
                years = response.css('.infobox td.plainlist a::text').extract()
                for line in years:
                    if len(line.strip()) == 4 and line[0:].isdigit():
                        year = line.strip()    
            if name is None or name == '-':
                name = response.css('.mw-page-title-main::text').extract_first()
            if name is None:
                name = response.css('.firstHeading.mw-first-heading i::text').extract_first()
            name = name.replace("\n", "")
            if year is None:
                year = ''
            else:
                year = year.replace("\n", "")
            data = [name, genre, director, country, year, url]
            all_data.append(data)
        else:
            data = [pre_name, 'Некорректно для серии', 'Некорректно для серии', 'Некорректно для серии', 'Некорректно для серии', url]
            all_data.append(data)

    def closed(self, response):
        global count
        global start_time
        global all_data
        stop = time.time()
        with open('movies.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerows(all_data)
        print(f'Обработано фильмов: {count} за {(stop - start_time)/60} минут')
        print('Создан файл movies.csv')