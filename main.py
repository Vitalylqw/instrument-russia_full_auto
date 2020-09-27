import os
import wget
import load_matrix as lm

answer = input('Введите Да, если хотите так же добавить новые данные на сайт')

PATH_PRICE_ADRESS = 'https://instrument.ru/yandexmarket/1b78da37-0b26-45a6-a885-095183509075.xml'
PATH_PRICE_NAME = 'yml_matrix.xml'
MY_SQL_IP  = '*****'
MY_SQL_USER = '*****'
MY_SQL_PASS = '****'
MY_SQL_DB_TEST = 'test_matrix'
MY_SQL_DB_WORK = 'instr_russia'
FILIE_NAME_CATRGORY = 'categorys.csv'
FTP_USER_NAME = '******'
FTP_HOST = '******'
FTP_PASS = '******'
FTP_ADRESS = '/www/instrument-russia.ru/image/catalog/photo/'
PHOTO_DIR = 'D:\photo'


print('Создадим свежий файл yml с данными поставщика')
# Создадим свежий файл yml с данными поставщика
try:
    os.remove(PATH_PRICE_NAME)
    print('Файл yml удален и будет скачан')
except:
    print('Файл yml не существует и  и будет скачан')
wget.download(PATH_PRICE_ADRESS,PATH_PRICE_NAME)

# Создадим классы для работы

# Класс для работы с файлом
file_data=lm.load_matrix_yml()

# Класс для работы с базо й данных
load = lm.Load_to_db_matrx()

# Получим даные категорий
data = file_data.get_category_data()

print('Загрузим данные в таблицу в БД')
# Загрузим данные в таблицу в БД
load.insert_by_data_into_category(data)
del (data)

# Получим остальные данные
products, images, atributs = file_data.get_other_data()

print('Загрузим товары')
# Загрузим товары
load.insert_by_data_into_import_data(products)
del (products)

print('загрузим атрибуты')
# загрузим атрибуты
load.insert_by_data_into_atributs(atributs)
del (atributs)
print('Загрузим таблицу с фото')
# Загрузим таблицу с фото
load.insert_by_data_into_photo(images)
del (images)
print('Обновим остатки и цены')
# Обновим остатки и цены
load.start_procedure_update_price()

if answer == 'Да':
    last_name_photo = load.get_last_photo_name()
    print(f'Последнее имя фото {last_name_photo}')
    print('Запустим процедуру добавления нового товара')
    load.start_procedure_update_products()
    new_product = load.get_count_new_product()
    print(f'Загруженно {new_product} новых товаров')
    if new_product>0:
        lm.load_foto(load)
        load.start_procedure_update_photo()
        lm.load_photo_to_site()
    print('Процесс обновления и загрузки нового товара закончен успешно')
else:
    print('Процесс обновления цен и остатокв товаров  успешно (без загрузки новых)')


    # Загрузим фото на сайт