import requests
import re
from bs4 import BeautifulSoup

from dateutil import parser
import pytz
import time
import random

import sys
sys.setrecursionlimit(2000)

class TechCrunchScraper:

    # Constructor:
    def __init__(self, url_base=None, categories=None, headers=None):
        self.url_base = url_base or 'https://techcrunch.com/'
        self.headers =  headers or {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv: 55.0) Gecko/20100101 Firefox/55.0',}
        self.categories = {
            'latest_news': 'latest/',
            # 'Startups':'category/startups/',
            # 'Ventures':'category/venture/',
            # 'Security':'category/security/',
            # 'AI':'category/artificial-intelligence/',
            # 'Apps':'category/apps/'
        }
        pass

    # Methods:
    def http_on_website(self, category, page):
        '''
        This method sends an http request on a url, once it is approved (status_code = 200), the html content is downloaded.
        The function finally returns a Beautiful Soup object.

        Include kwargs to get the option of watching the connection headers or the cookies included.
        '''
        try:
            if page is not None:
                url = self.url_base + category + 'page/' + str(page) + '/'
                print(url)
            else:
                url = self.url_base + category
                print(url)
            resource = requests.get(url, headers=self.headers)
            print(f"Conexión exitosa: {resource.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Conexión fallida: {e}")

        #     # Connection headers
        # headers_dict = resource.headers

        # for key, value in headers_dict.items():
        #     print(f"{key}: {value}")

        # # Cookies
        # cookies_dict = resource.cookies
        # print(cookies_dict)

        # for key, val in cookies_dict.items():
        #     print(f"{key}: {val}")

        html = resource.content
        soup = BeautifulSoup(html, 'html.parser')
        
        return soup

    def __datetime_news_publish(self, soup_noticia):
        '''
        This function takes a Beautiful Soup object, extracts the tag 'time' and processes it in order to obtain local datetime and utc datetime.
        '''
        # Fecha publicacion:
        fecha_noticia = soup_noticia.find_all('time')

        fecha_hora_publicacion_texto = fecha_noticia[0].text
        lista_publicacion_texto = fecha_hora_publicacion_texto.split(' · ')
        hora_publicacion_texto = lista_publicacion_texto[0]
        fecha_publicacion_texto = lista_publicacion_texto[1]

        # Convertir a formato datetime:
        fecha_hora_str = f"{fecha_publicacion_texto} {hora_publicacion_texto}"

        # Parsear string a datetime con dateutil:
        dt_sin_tz = parser.parse(fecha_hora_str, ignoretz=True)

        # Crear timezone de Pacific:
        pacific = pytz.timezone('US/Pacific')

        # Localizar el datetime (asignar zona horaria correcta):
        dt_localizado = pacific.localize(dt_sin_tz)
        dt_utc = dt_localizado.astimezone(pytz.utc)

        return [dt_localizado, dt_utc]

    def __data_extraction_news(self, i, link_noticias, link_category, link_author):
        '''
        This function generates a dictionary which maps the headtitle, the name of the author, the link and the headtitle to be stored afterwards.
        '''
        # Extraer Categoría:
        main_category = link_category[i].text # Modificar indice para acceder a todos

        # Titular y link a la noticia:
        link = link_noticias[i]['href']
        headtitle = link_noticias[i].text

        # Autor de la noticia:
        news_author = link_author[i].text

        # data_extraction_pagenews(link)
        output_dict = {'main_category':main_category, 'headtitle':headtitle, 'news_author':news_author, 'link':link}
        return output_dict

    # Entrar en cada url de cada noticia:
    def __data_extraction_pagenews(self, link):
        """
        Extracts detailed information from an individual TechCrunch news article page.

        Given a news article URL (`link`), this method downloads the page and parses its content to extract:
        - The full article body text
        - The list of relevant topics/tags
        - The publication date and time (both localized and UTC)

        Returns a dictionary with the extracted information. If extraction fails, returns an empty dictionary.
        """
        try:
            request_noticia = requests.get(link, headers=self.headers)
            if request_noticia.status_code != 200:
                print(f"Conexión fallida: status code {request_noticia.status_code} for {link}")
                return {}
            # print(f"Conexión exitosa: {request_noticia.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Conexión fallida: {e}")
            return {}

        html_noticia = request_noticia.content   
        soup_noticia = BeautifulSoup(html_noticia, 'html.parser')

        # Cuerpo noticia
        pagina_noticia = soup_noticia.find_all('div', class_="entry-content")
        cuerpo_noticia_str = ''

        if pagina_noticia and len(pagina_noticia) > 0:
            cuerpo_noticia = pagina_noticia[0].find_all('p')
            if cuerpo_noticia:
                for i in cuerpo_noticia:
                    cuerpo_noticia_str += str(i.text) + '\n\n'
        else:
            print(f"Advertencia: No se encontró el div 'entry-content' en {link}")

        # List of topics
        topics = soup_noticia.find_all('div', class_="tc23-post-relevant-terms__terms")
        if topics:
            list_of_topics = topics[0].text.replace('\n','')
        else:
            list_of_topics = ''

        # Fecha publicacion
        try:
            datetime_list = self.__datetime_news_publish(soup_noticia)
            dt_localizado_str = datetime_list[0].strftime('%Y-%m-%d %H:%M:%S %Z%z')
            dt_utc_str = datetime_list[1].strftime('%Y-%m-%d %H:%M:%S %Z%z')
        except Exception as e:
            print(f"Advertencia: No se pudo extraer la fecha de publicación en {link}: {e}")
            dt_localizado_str = ''
            dt_utc_str = ''

        output_dict = {
            'dt_localizado':dt_localizado_str,
            'dt_utc': dt_utc_str,
            'cuerpo_noticia_str':cuerpo_noticia_str,
            'list_of_topics':list_of_topics
        }

        return output_dict

    def recursive_data_process(self, soup, i=0, lista_merge_dict=None, ul6=None, link_noticias=None, link_category=None, link_author=None):
        '''
        Purpose: the function extracts and processes news articles from a TechCrunch category page,
        collecting detailed information for each news item into a list of dictionaries.

        Output: returns a list of dictionaries, each containing all extracted information for a news
        article.
        '''
        
        if lista_merge_dict is None:
            lista_merge_dict = []

        # Obtener la lista de noticias solo una vez:
        if ul6 is None:
            ul = soup.find_all('ul')
            if len(ul) > 6:
                ul6 = ul[6]
            else:
                print("Error: Less than 7 <ul> elements found in the page.")
                return lista_merge_dict
            # Extraer noticias
            link_noticias = ul6.find_all('a', class_="loop-card__title-link") # alternativa: attrs={"class"="loop-card__title-link", "href":True}
            # Extraer Categoría de noticia:
            link_category = ul6.find_all('a', class_="loop-card__cat") # alternativa: attrs={"class"="loop-card__cat", "href":True}
            # Extraer autor/a de la noticia:
            link_author = ul6.find_all('a', class_="loop-card__author") # alternativa: attrs={"class"="loop-card__author", "href":True}

        # Caso base: detener si i excede el número de noticias:
        if link_noticias is None:
            link_noticias = []
        num_noticias = len(link_noticias)

        if i >= num_noticias:
            return lista_merge_dict
        
        try:
            output_dict = self.__data_extraction_news(i, link_noticias, link_category, link_author)
            output_page_dict = self.__data_extraction_pagenews(output_dict['link'])

            merge_dict = output_dict | output_page_dict
            lista_merge_dict.append(merge_dict)

            # Sleep para cada iteración para evitar colapsar a base de peticiones
            delay = max(0, random.gauss(1.4,0.15)) # modificar en caso de multithreading
            time.sleep(delay)

        except Exception as e:
            print(f"Error en la iteracion{i} : {e}")
            print(f"Conexión fallida: {type(e).__name__}: {e}")
            # merge_dict = {**output_dict, **output_page_dict}
            # lista_merge_dict.append(merge_dict)

        return self.recursive_data_process(soup, i+1, lista_merge_dict)