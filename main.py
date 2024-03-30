import logging
import re
import sys

import requests
from bs4 import BeautifulSoup
from peewee import MySQLDatabase, Model, BigAutoField, CharField, IntegerField, DecimalField, ForeignKeyField

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("myapp.log")
logger.addHandler(file_handler)

mysql_db = MySQLDatabase('geoadm', user='geoadm', password='geoadm', host='127.0.0.1', port=3306)


class BaseModel(Model):
    class Meta:
        database = mysql_db


class FederalDistrict(BaseModel):
    id = BigAutoField(primary_key=True, unique=True)
    name = CharField(max_length=255, null=False)
    name_short_en = CharField(max_length=10, null=False)
    name_short_ru = CharField(max_length=10, null=False)


class Region(BaseModel):
    id = BigAutoField(primary_key=True, unique=True)
    name = CharField(max_length=255, null=False)
    adm_center = CharField(max_length=255, null=True)
    population = IntegerField(null=True)
    percent_population = DecimalField(decimal_places=2, null=True)
    federal_district = ForeignKeyField(FederalDistrict, backref="regions", null=True)
    population_density = DecimalField(decimal_places=2, null=True)
    subject_code = IntegerField(null=True)
    phone_code = CharField(max_length=255, null=True)


class Area(BaseModel):
    id = BigAutoField(primary_key=True, unique=True)
    name = CharField(max_length=255, null=False)
    region = ForeignKeyField(Region, backref="areas", null=True)


class Locality(BaseModel):
    id = BigAutoField(primary_key=True, unique=True)
    name = CharField(max_length=255, null=False)
    locality_type = CharField(max_length=50, null=True)
    population = IntegerField(null=True)
    area = ForeignKeyField(Area, backref="localities", null=True)


mysql_db.create_tables([FederalDistrict, Region, Area, Locality])

federal_district_values = [
    ("Центральный федеральный округ", "tsfo", "цфо"),
    ("Приволжский федеральный округ", "pfo", "пфо"),
    ("Сибирский федеральный округ", "sfo", "сфо"),
    ("Южный федеральный округ", "yufo", "юфо"),
    ("Северо-Западный федеральный округ", "szfo", "сзфо"),
    ("Уральский федеральный округ", "ufo", "урфо, уфо"),
    ("Северо-Кавказский федеральный округ", "skfo", "скфо"),
    ("Дальневосточный федеральный округ", "dfo", "двфо, дфо"),
]

FederalDistrict.truncate_table()
FederalDistrict.insert_many(federal_district_values, fields=[FederalDistrict.name, FederalDistrict.name_short_en,
                                                             FederalDistrict.name_short_ru]).execute()


def parse_regions(regions_html):
    regions_result = []
    soup = BeautifulSoup(regions_html, "html.parser")
    table = soup.find("table", class_="table-bordered")
    if not table:
        logger.error("Не удалось найти таблицу регионов")
        sys.exit(-1)
    for row in table.find_all("tr"):
        if "% от общего населения РФ" in row.text:
            continue
        region = {
            "name": None,
            "adm_center": None,
            "population": None,
            "percent": None,
            "district": None,
            "density": None,
            "locality_url": None,
        }
        region_keys = list(region.keys())
        for idx, ceil in enumerate(row.find_all("td")):
            key = region_keys[idx]
            if idx == 0:
                link = ceil.find("a")
                if link:
                    region["locality_url"] = link.get("href")
            if len(ceil.text) > 0:
                region[key] = ceil.text
        regions_result.append(region)
    logger.info(f"Получил {regions_result.__len__()} регионов")
    return regions_result


def get_regions():
    logger.info("Получаю регионы")
    try:
        res = requests.get("https://geoadm.com/")
        if res.status_code == 200:
            regions_html = res.text
            return parse_regions(regions_html)
    except Exception as e:
        logger.exception(e)
        logger.error("Ошибка при получении регионов")
        sys.exit(-1)


def modify_regions_data(regions):
    for region in regions:
        if region["percent"]:
            region["percent"] = float(region["percent"].replace(",", "."))
        if region["density"]:
            region["density"] = float(region["density"].replace(",", "."))
        if region["population"]:
            region["population"] = str_to_int(region["population"])
        if region["district"]:
            region["district"] = region["district"].lower()
    return regions


def save_regions(regions):
    logger.info("Сохраняю регионы")
    Region.truncate_table()
    for region in regions:
        federal_district = None
        try:
            federal_district = FederalDistrict.select().where(
                FederalDistrict.name_short_ru.contains(region["district"])).get()
        except FederalDistrict.DoesNotExist:
            pass

        region_record = Region.create(name=region["name"], adm_center=region["adm_center"],
                                      population=region["population"],
                                      percent_population=region["percent"], population_density=region["density"],
                                      federal_district=federal_district)
        region["region_record"] = region_record
    return regions


def get_localities(regions):
    logger.info("Получаю страницы населенных пунктов")
    localities_pages = []
    for region in regions:
        if region["locality_url"]:
            try:
                res = requests.get(f"https://geoadm.com{region['locality_url']}")
                if res.status_code == 200:
                    localities_pages.append({
                        "region_record": region["region_record"],
                        "page_html": res.text,
                    })
            except Exception as e:
                logger.exception(e)
                logger.error("Ошибка при получении населенных пунктов")
                sys.exit(-1)
    logger.info(f"Получил {localities_pages.__len__()} страниц населенных пунктов")
    return parse_localities(localities_pages)


def parse_localities(localities_pages):
    localities_result = []
    regions_additionally = []
    for locality_page in localities_pages:
        soup = BeautifulSoup(locality_page["page_html"], "html.parser")
        tables = soup.find_all("table", class_="table-bordered")
        if not tables or len(tables) != 2:
            logger.info(locality_page["region_record"])
            logger.error("Ошибка неверное количество таблиц на странице населенных пунктов")
            sys.exit(-1)
        ul_items = soup.find_all("ul")
        if ul_items and len(ul_items) > 0:
            li_items = ul_items[0].find_all("li")
            if li_items and len(li_items) > 0:
                li_items_text = []
                for li_item in li_items:
                    li_items_text.append(li_item.text)
                regions_additionally.append({
                    "region_record": locality_page["region_record"],
                    "li_items_text": li_items_text,
                })
        table = tables[1]
        for row in table.find_all("tr"):
            if "Название" in row.text:
                continue
            locality_item = {
                "name": None,
                "type": None,
                "population": None,
                "area_name": None,
                "region_record": locality_page["region_record"],
            }
            locality_keys = list(locality_item.keys())
            for idx, ceil in enumerate(row.find_all("td")):
                if idx == 0:
                    continue
                key = locality_keys[idx - 1]
                if len(ceil.text) > 0:
                    locality_item[key] = ceil.text
            localities_result.append(locality_item)
    logger.info(
        f"Получил {localities_result.__len__()} населенных пунктов. Дополнительных данных региона {regions_additionally.__len__()}")
    return localities_result, regions_additionally


def modify_localities_data(pack_parameters):
    localities, regions_additionally = pack_parameters
    for locality in localities:
        if locality["population"] is not None or locality["type"] is not None:
            # столбцы население и тип перепутаны местами на некоторых страницах
            locality_type = locality["type"]
            locality_population = locality["population"]
            try:
                locality["population"] = str_to_int(locality["population"])
            except Exception:
                try:
                    locality["population"] = str_to_int(locality["type"])
                    locality["type"] = locality_population
                except Exception:
                    if len(locality_type) > 2:
                        locality["type"] = locality_type
                        locality["population"] = locality_population
                    else:
                        locality["type"] = locality_population
                        locality["population"] = locality_type
    for region_additionally in regions_additionally:
        for li_item_text in region_additionally["li_items_text"]:
            if "Код субъекта России" in li_item_text:
                region_additionally["subject_code"] = str_to_int(li_item_text)
            if "Телефонный код" in li_item_text:
                region_additionally["phone_code"] = ", ".join(re.findall(int_parse_regexp, li_item_text))
    return localities, regions_additionally


def save_localities(pack_parameters):
    Area.truncate_table()
    Locality.truncate_table()
    logger.info("Сохраняю населенные пункты")
    localities, regions_additionally = pack_parameters
    for locality in localities:
        area = None
        if locality["area_name"]:
            try:
                area = Area.select().where(
                    (Area.name == locality["area_name"]) & (Area.region == locality["region_record"])).get()
            except Area.DoesNotExist:
                area = Area.create(name=locality["area_name"], region=locality["region_record"])
        if locality["name"] is None:
            logger.info(locality)
        Locality.create(name=locality["name"], locality_type=locality["type"], population=locality["population"],
                        area=area)
    for region_additionally in regions_additionally:
        if "subject_code" in region_additionally or "phone_code" in region_additionally:
            try:
                region = Region.select().where(Region.id == region_additionally["region_record"].id).get()
            except Region.DoesNotExist:
                continue
            if "subject_code" in region_additionally:
                region.subject_code = region_additionally["subject_code"]
            if "phone_code" in region_additionally:
                region.phone_code = region_additionally["phone_code"]
            region.save()


int_parse_regexp = r"\b\d+\b"


def str_to_int(string_int):
    return int("".join(re.findall(int_parse_regexp, string_int)))


def main():
    regions = save_regions(modify_regions_data(get_regions()))
    save_localities(modify_localities_data(get_localities(regions)))
    logger.info(
        f"Итог. Федеральные округа: {FederalDistrict.select().count()}"
        f" Регионы: {Region.select().count()} Округа: {Area.select().count()}"
        f" Населенные пункты: {Locality.select().count()}")


if __name__ == "__main__":
    main()
