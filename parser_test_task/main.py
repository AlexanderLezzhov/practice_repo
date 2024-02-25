import json
import psycopg2 
import psycopg2.extras as extras 
import pandas as pd
from parser import Parser
from dataframe_prep import Dataframe_prep


if __name__ == '__main__':

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        # 'Cookie': '_ym_uid=1708362913965016894; _ym_d=1708362913; _ym_isad=2; qrator_msid=1708423696.097.sGxY0VlzaVIYCMg7-a4qsdh4712lei02nnvoo9d7fsn5upuea; _ym_visorc=b',
        'Pragma': 'no-cache',
        'Referer': 'https://fedresurs.ru',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    start_dt_str = '2023-05-15' #'2020-11-25' '2023-05-15' 
    end_dt_str = '2023-12-18' # '2023-11-20' '2023-12-18'

    strict_start_str = '2023-05-15T17:16:48.000' # '2020-11-25T09:44:27.000' '2023-05-15T17:16:48.000'
    strict_end_str = '2023-12-18T13:36:17.999' #'2023-11-20T16:34:26.999' '2023-12-18T13:36:17.999'

    name_ru = 'АО Каршеринг' #'ООО Каршеринг Руссия' 'АО Каршеринг'
    name_en = 'belka' #'deli' 'belka'

    parser_obj = Parser(headers)

    #Поиск ссылки на сообщения ЮЛ интерактивно в браузере

    legal_entity_url_mask = parser_obj.legal_entity_url_parse(name_ru)
    print(legal_entity_url_mask)

    #Сбор атрибутов сообщений в заданных датах

    message_links_data = parser_obj.messages_links_parse(start_dt_str, end_dt_str, legal_entity_url_mask)

    #Парсинг объектов сообщений 

    message_links_data_cleared = [i for i in message_links_data if i.get('datePublish') > strict_start_str
                                     and i.get('datePublish') <= strict_end_str]

    messege_subjects = parser_obj.messages_parse_by_links(message_links_data_cleared)

    #Сохранение объектов сообщений 

    with open(f'{name_en}_messages.json', 'w') as fp:
         json.dump(messege_subjects, fp, ensure_ascii=False)

    #Чтение json-файла и преобразование его в датафрейм с последующими модификациями

    df_creator = Dataframe_prep()

    with open(f'{name_en}_messages.json') as json_file:
        messege_subjects_dict = json.load(json_file)

    final_df = df_creator.df_creator(messege_subjects_dict)

    #Сохранение в csv

    final_df.to_csv(f'{name_en}_parsed_test.csv', sep = ";", encoding = "utf=8-sig", index = False)

    #Загрузка в БД

    df_for_db = pd.read_csv(f'{name_en}_parsed_test.csv', sep = ";", encoding = "utf-8-sig")
    df_for_db['carshering_operator'] = f'{name_ru}'
    tuples = [tuple(x) for x in df_for_db.to_numpy()] 
    tuples = [tuple(None if i != i else i for i in x) for x in tuples]

    create_tb_query = """
        CREATE TABLE if not exists public.carsharing_vehicles (
              guid text NULL
            , lessors text NULL
            , lessors_inn text NULL
            , lessors_ogrn text NULL
            , identifier text NULL
            , classifier text NULL
            , description text NULL
            , vin_id_corrected text NULL
            , date_publish timestamp(6)
            , carsharing_operator text
        
        );
        """
    insert_tb_query = "INSERT INTO %s(%s) VALUES %%s" % ('public.carsharing_vehicles', 'guid, lessors, lessors_inn, lessors_ogrn, identifier, classifier, description, vin_id_corrected, date_publish, carsharing_operator') 

    conn = psycopg2.connect(database="PostgreSQL-9110", user='user', password='********', host='212.233.97.210', port='5432')
    cursor = conn.cursor()

    cursor.execute(create_tb_query)
    conn.commit()
    extras.execute_values(cursor, insert_tb_query, tuples)
    conn.commit()
    cursor.close()




