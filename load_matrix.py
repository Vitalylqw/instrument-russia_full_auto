from lxml import etree
import wget
import pymysql
import csv
from ftplib import FTP
import os

# Определим основные переменные
PATH_PRICE_ADRESS = 'https://instrument.ru/yandexmarket/1b78da37-0b26-45a6-a885-095183509075.xml'
PATH_PRICE_NAME = 'yml_matrix.xml'
MY_SQL_IP  = '******
MY_SQL_USER = '*******'
MY_SQL_PASS = '******'
MY_SQL_DB_TEST = 'test_matrix'
MY_SQL_DB_WORK = 'instr_russia'
FILIE_NAME_CATRGORY = 'categorys.csv'
FTP_USER_NAME = '****************'
FTP_HOST = '***************'
FTP_PASS = '*****************'
FTP_ADRESS = '/www/instrument-russia.ru/image/catalog/photo/'
PHOTO_DIR = 'D:\photo'


class load_matrix_yml():
    """ Класс для работы с файлом данных поставщика в формате yml"""
    def __init__(self,f_name=PATH_PRICE_NAME):
        self.f_name=f_name
        self.tree = etree.parse(self.f_name)
    def get_all_data_files(self):
        products, images, atributs =self.__get_products()
        # Запишем файл с продукцией
        with open('products.csv', 'w',encoding='utf-8') as f:
            f.write('model' + ';')
            f.write('name' + ';')
            f.write('quantity' + ';')
            f.write('manufacturer' ';')
            f.write('min' + ';')
            f.write('price' + ';')
            f.write('categoryId' + ';')
            f.write('width' + ';')
            f.write('photo' + ';')
            f.write('description'  + '\n')
            for i in products:
                f.write(i['model']+';')
                f.write("'"+i ['name'] +"'"+ ';')
                f.write(i['quantity'] + ';')
                f.write(i['manufacturer'] + ';')
                f.write(i['min'] + ';')
                f.write(i['price'] + ';')
                f.write(i['categoryId'] + ';')
                f.write(i['width'] + ';')
                f.write("'"+i['photo'] +"'"+ ';')
                f.write("'"+i['description']+"'" + '\n')
        # Запишем файл с картинками
        with open('images.csv', 'w', encoding='utf-8') as f:
            f.write('model' + ';')
            f.write('image' + '\n')
            for i in images:
                list_images = i['links']
                for j in list_images:
                    f.write(i['model'] + ';')
                    f.write("'" +j+ "'"+ '\n')
        # Запишем файл с атрибутами

        with open('atributs.csv', 'w', encoding='utf-8') as f:
            #f.write('id' + ';')
            f.write('model' + ';')
            f.write('name' + ';')
            f.write('value' + '\n')
            for i in atributs:
                dict_params = i['params']
                for key,val in dict_params.items():
                    f.write(i['model'] + ';')
                    f.write("'" +key+"'" + ';')
                    f.write("'" +val+"'"+ '\n')

    def get_count(self):
        data = self.tree.xpath('//offer')
        return len(data)

    def __get_products(self):
        products=[]
        images = []
        atributs = []
        data = self.tree.xpath('//offer')
        for i in data:
            product = {}
            image = {}
            atribut = {}
            photos_links = []
            params = {}
            product['min']='1'
            product['width']='0'
            children=i.getchildren()
            if i.attrib['available'] == 'true':
                product['quantity'] = '100'
            for tag in children:
                if tag.tag == 'shop-sku':
                    product['model'] =  tag.text
                    image['model'] = tag.text
                    atribut['model'] = tag.text
                elif tag.tag == 'price':
                    product['price'] = tag.text
                elif tag.tag == 'categoryId':
                    product['categoryId'] =tag.text
                elif tag.tag == 'picture':
                    photos_links.append(str(tag.text))
                elif tag.tag == 'name':
                    product['name'] =(tag.text)
                    product['description'] =(tag.text)
                elif tag.tag == 'vendor':
                    product['manufacturer'] =(tag.text)
                elif tag.tag == 'weight':
                    product['width'] =(tag.text)
                elif tag.tag == 'description':
                    product['description'] =(tag.text)
                elif tag.tag == 'param':
                    if tag.attrib['name'] == 'Мин. упак':
                        product['min']=tag.text
                    elif '_' in  tag.attrib['name'] or tag.attrib['name'] == 'Штрихкод' or  'груп' in tag.attrib['name']:
                        pass
                    else:
                        params[tag.attrib['name']] = tag.text
            product['photo']=photos_links[0]
            image['links'] = photos_links
            atribut['params']=params
            products.append(product)
            images.append(image)
            atributs.append(atribut)
        return (products, images, atributs)

    def get_category_in_file(self):
        data = self.tree.xpath('//category')
        categorys=[]
        for i in data:
            category = {}
            category['id']=i.attrib['id']
            category['name'] = i.text
            try:
                category['parentId'] = i.attrib['parentId']
            except:
                category['parentId'] = '0'
            categorys.append(category)
        with open('categorys.csv', 'w', encoding='utf-8') as f:
            f.write('category_id' + ';')
            f.write('name' + ';')
            f.write('parentId' + '\n')
            for i in categorys:
                f.write(i['id']+ ';')
                f.write("'"+i['name']+"'" + ';')
                f.write(i['parentId']+ '\n')

    def get_category_data(self):
        """Возвращает список кортеджей для вставки в БД
            в следующем порядке id, name,parentID"""
        data = self.tree.xpath('//category')
        categorys=[]
        for i in data:
            cat_id =i.attrib['id']
            cat_name = i.text
            try:
                cat_parent = i.attrib['parentId']
            except:
                cat_parent = '0'
            category=(cat_id,cat_name,cat_parent)
            categorys.append(category)
        return categorys


    def get_other_data(self):
        products=[]
        images = []
        atributs = []
        data = self.tree.xpath('//offer')
        for i in data:
            product = {}
            image = {}
            atribut = {}
            photos_links = []
            params = {}
            product['min']='1'
            product['width']='0'
            children=i.getchildren()
            if i.attrib['available'] == 'true':
                product['quantity'] = '100'
            for tag in children:
                if tag.tag == 'shop-sku':
                    product['model'] =  tag.text
                    image['model'] = tag.text
                    atribut['model'] = tag.text
                elif tag.tag == 'price':
                    product['price'] = tag.text
                elif tag.tag == 'categoryId':
                    product['categoryId'] =tag.text
                elif tag.tag == 'picture':
                    photos_links.append(str(tag.text))
                elif tag.tag == 'name':
                    product['name'] =(tag.text)
                    product['description'] =(tag.text)
                elif tag.tag == 'vendor':
                    product['manufacturer'] =(tag.text)
                elif tag.tag == 'weight':
                    product['width'] =(tag.text)
                elif tag.tag == 'description':
                    product['description'] =(tag.text)
                elif tag.tag == 'param':
                    if tag.attrib['name'] == 'Мин. упак':
                        product['min']=tag.text
                    elif '_' in  tag.attrib['name'] or tag.attrib['name'] == 'Штрихкод' or  'груп' in tag.attrib['name']:
                        pass
                    else:
                        params[tag.attrib['name']] = tag.text
            product['photo']=photos_links[0]
            image['links'] = photos_links
            atribut['params']=params
            products.append(product)
            images.append(image)
            atributs.append(atribut)
        return (products, images, atributs)






class Load_to_db_matrx():
    """Класс для работы с базой данных"""

    def __init__(self,ip =MY_SQL_IP ,user=MY_SQL_USER,pas = MY_SQL_PASS,db_w=MY_SQL_DB_WORK,db_test =MY_SQL_DB_TEST ):
        self.ip = ip
        self.user = user
        self.pas = pas
        self.db_w = db_w
        self.db_test = db_test

    def connect(self):
        # К базе данных магазина
        try:
            self.conn_mysql = pymysql.connect(self.ip, self.user, self.pas, self.db_test)
            self.cursor_mysql = self.conn_mysql.cursor()
        except:
            print('Не удалось подключиться к базе магазина')
            self.conn_mysql.close()
            raise
        else:
            print('Подключение к БД магазина удалось')

    def insert_by_data_into_category(self,data):
        self.connect()
        self.cursor_mysql.execute("""TRUNCATE test_matrix.category""")
        self.cursor_mysql.executemany('INSERT INTO test_matrix.category (category_id, name, parentId) VALUES(%s, %s,%s)', data)
        self.conn_mysql.commit()
        print(f'Загруженно в категории {len(data)} элементов')
        self.cursor_mysql.close()
        self.conn_mysql.close()

    def insert_by_file_into_category(self,file_name=FILIE_NAME_CATRGORY):
        self.connect()
        self.cursor_mysql.execute("""TRUNCATE test_matrix.category""")
        with open(file_name, 'r', encoding='utf-8') as f:
            csv_data = csv.reader(f,delimiter = ";",quotechar ="'")
            next(csv_data)
            n=0
            for row in csv_data:
                n+=1
                self.cursor_mysql.execute('INSERT INTO test_matrix.category (category_id, name, parentId) VALUES(%s, %s,%s)', row)
        self.conn_mysql.commit()
        print(f'Во временную таблицу ктегорий было вставленно {n} записей')
        self.cursor_mysql.close()
        self.conn_mysql.close()

    def insert_by_data_into_import_data(self,data):
        self.connect()
        self.cursor_mysql.execute("""TRUNCATE test_matrix.import_data""")
        self.cursor_mysql.executemany('INSERT INTO test_matrix.import_data  VALUES(Null,%(model)s, %(name)s,%(quantity)s,%(manufacturer)s, %(min)s,%(price)s,%(categoryId)s, %(width)s,%(photo)s,%(description)s)', data)
        self.conn_mysql.commit()
        print(f'Загруженно в товары {len(data)} элементов')
        self.cursor_mysql.close()
        self.conn_mysql.close()

    def insert_by_data_into_atributs(self,data):
        atr=[]
        for i in data:
            model = i['model']
            for n,m in i['params'].items():
                atrib={}
                atrib['model'] = model
                atrib['name'] = n
                atrib['value'] = m
                atr.append(atrib)
        self.connect()
        self.cursor_mysql.execute("""TRUNCATE test_matrix.atributs""")
        self.cursor_mysql.executemany('INSERT INTO test_matrix.atributs(model,name,value)  VALUES(%(model)s, %(name)s,%(value)s)', atr)
        self.conn_mysql.commit()
        print(f'Загруженно атрибутов {len(atr)} элементов')
        self.cursor_mysql.close()
        self.conn_mysql.close()
        del (atr)

    def insert_by_data_into_photo(self,data):
        photo=[]
        for i in data:
            model = i['model']
            for j in i['links']:
                phot={}
                phot['model'] = model
                phot['url'] = j
                photo.append(phot)
        self.connect()
        self.cursor_mysql.execute("""TRUNCATE test_matrix.photo""")
        self.cursor_mysql.executemany('INSERT INTO test_matrix.photo (model,url)  VALUES( %(model)s, %(url)s)', photo)
        self.conn_mysql.commit()
        print(f'Загруженно ссылок на фото {len(photo)} элементов')
        self.cursor_mysql.close()
        self.conn_mysql.close()
        del (photo)

    def start_procedure_update_price(self):
        self.connect()
        try:
            self.cursor_mysql.execute("""CALL instr_russia.update_price_count()""")
        except Exception as a:
            print('Процедура обновления остаток и цен не выполненна')
            print(a)
            self.cursor_mysql.close()
            self.conn_mysql.close()
        else:
            print('Процедура обновления остаток и цен  выполненна!!!')
        self.cursor_mysql.close()
        self.conn_mysql.close()

    def get_last_photo_name(self):
        self.connect()
        self.cursor_mysql.execute('SELECT name FROM test_matrix.photo_name ORDER BY name DESC limit 1;')
        answer = self.cursor_mysql.fetchone()[0]
        self.cursor_mysql.close()
        self.conn_mysql.close()
        return answer

    def start_procedure_update_products(self):
        self.connect()
        try:
            self.cursor_mysql.execute("""CALL instr_russia.update_products""")
        except Exception as a:
            print('Процедура добавления товаров не выполненна')
            print(a)
            self.cursor_mysql.close()
            self.conn_mysql.close()
        else:
            print('Процедура одобавления товаров выполненна!!!')
        self.cursor_mysql.close()
        self.conn_mysql.close()

    def get_count_new_product(self):
        self.connect()
        self.cursor_mysql.execute('SELECT COUNT(*) FROM test_matrix.lost_products;')
        answer = self.cursor_mysql.fetchone()[0]
        self.cursor_mysql.close()
        self.conn_mysql.close()
        return answer

    def new_photo_product(self):
        self.connect()
        self.cursor_mysql.execute("""SELECT lp.model, url FROM test_matrix.lost_products lp 	join photo p ON lp.model = p.model WHERE  url <> 'None';""")
        answer = self.cursor_mysql.fetchall()
        self.cursor_mysql.close()
        self.conn_mysql.close()
        return answer

    def insert_photo_name(self,data):
        self.connect()
        self.cursor_mysql.executemany('insert into test_matrix.photo_name VALUES(Null,%s,%s);',data)
        self.conn_mysql.commit()
        self.cursor_mysql.close()
        self.conn_mysql.close()

    def start_procedure_update_photo(self):
        self.connect()
        try:
            self.cursor_mysql.execute("""CALL instr_russia.update_photo""")
        except Exception as a:
            print('Процедура добавления новых фото не выполненна')
            print(a)
            self.cursor_mysql.close()
            self.conn_mysql.close()
        else:
            print('Процедура добавления новых фото выполненна!!!')
        self.cursor_mysql.close()
        self.conn_mysql.close()


def load_foto(obj):
    for file in os.listdir(PHOTO_DIR):
        path = f'{PHOTO_DIR}\{file}'
        os.remove(path)
    n=obj.get_last_photo_name()+1
    data = []
    for str in obj.new_photo_product():
        try:
            wget.download(str[1], f'D:\photo\{n}.jpg')
            dat=(str[0],n)
            data.append(dat)
            n+=1
        except Exception as e:
            print(f'''Не смогли скачать {str[1]}''',e ,str, sep='\n')
    obj.insert_photo_name(data)

def load_photo_to_site():
    ftp = FTP(FTP_HOST,FTP_USER_NAME,FTP_PASS)
    ftp.cwd(FTP_ADRESS)
    for file in os.listdir(PHOTO_DIR):
        path = f'{PHOTO_DIR}\{file}'
        with open(path, 'rb') as f:
            print(f)
            send = ftp.storbinary('STOR '+file,f)
            print(f'файл {file} загружен')



if __name__=='__main__':
    pass


