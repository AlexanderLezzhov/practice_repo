from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import requests
import json

class Parser():
    def __init__(self, headers):
        self.headers = headers

    def legal_entity_url_parse(self, legal_entity_parse):
        find_input_xpath = '/html/body/fedresurs-app/div[1]/home/div/quick-search/div/div/form/input'
        find_inner_input_xpath = '/html/body/fedresurs-app/div[1]/search/div/div/div/entity-search/div[1]/div/div/form/input-field[2]/div/input'
        active_legal_entity_mark = '//*[@id="onlyActive"]'
        find_button_xpath = '/html/body/fedresurs-app/div[1]/home/div/quick-search/div/div/form/button'
        find_button_inner_xpath = '/html/body/fedresurs-app/div[1]/search/div/div/div/entity-search/div[1]/div/div/form/button'
        try:
            try:
                browser = webdriver.Firefox()
                browser.get('https://fedresurs.ru/')
                time.sleep(2)
                browser.find_element("xpath", find_input_xpath).send_keys(legal_entity_parse)
                browser.find_element("xpath", find_button_xpath).click()
                time.sleep(2)
                legal_entity_url = browser.find_element("class name", 'td_company_name').get_attribute('href')
                #browser.get(legal_entity_url)
                return legal_entity_url.split('/')[-1]
            except:
                browser.find_element("xpath", find_inner_input_xpath).clear()
                time.sleep(2)
                browser.find_element("xpath", active_legal_entity_mark).click()
                time.sleep(2)
                browser.find_element("xpath", find_inner_input_xpath).send_keys(legal_entity_parse)
                browser.find_element("xpath", find_button_inner_xpath).click()
                legal_entity_url = browser.find_element("class name", 'td_company_name').get_attribute('href')
                #browser.get(legal_entity_url)
                return legal_entity_url.split('/')[-1]  
        except NoSuchElementException:
            browser.close()
            return 'Та шо-то нэмае'
        finally:
            browser.close()
        
    def messages_links_parse(self, start_dt_str, end_dt_str, legal_entity_url_mask):  
    
        messages_url_parse_base = 'https://fedresurs.ru/backend/companies/{0}/publications?limit=15&offset={1}&startDate={2}T00:00:00.000Z&endDate={3}T00:00:00.000Z&searchCompanyEfrsb=true&searchAmReport=true&searchFirmBankruptMessage=true&searchFirmBankruptMessageWithoutLegalCase=false&searchSfactsMessage=true&searchSroAmMessage=true&searchTradeOrgMessage=true'
        ts_to_date = lambda x: datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S').date()
        
        start_dt = datetime.strptime(start_dt_str, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_dt_str, '%Y-%m-%d').date()
        calendar_days = []
        message_links_data = []

        while start_dt<end_dt:
            local_fin_pre = start_dt + relativedelta(days=30) - relativedelta(days=1)
            local_fin = local_fin_pre if local_fin_pre < end_dt else end_dt
            calendar_days.append([str(start_dt), str(local_fin)])
            start_dt = local_fin + relativedelta(days=1)

        for interval in calendar_days:
            offset_base = 0
            dt_start_inc,  dt_end_inc = interval[0], interval[1]
            #print(dt_start_inc,  dt_end_inc)
            r = requests.get(messages_url_parse_base.format(legal_entity_url_mask, offset_base, dt_start_inc,  dt_end_inc)
                            , headers = self.headers )
            if r.status_code != 200:
                print(r.text)
                break
            elif r.json()['found'] == 0:
                print("No data in dates: ", dt_start_inc, dt_end_inc)
                pass
            else:
                mes_cnt = r.json()['found'] if r.json()['found'] < 510 else 510
                offsets = list(range(0, mes_cnt, 15))
                #print(r.json()['found'])
                for i in offsets:
                    r_loc = requests.get(messages_url_parse_base.format(legal_entity_url_mask, i, dt_start_inc,  dt_end_inc)
                            , headers = self.headers )
                    #print(len(r_loc.json()['pageData']))
                    message_links_data += r_loc.json()['pageData']
            
        return message_links_data
    
    def messages_parse_by_links(self, links_jsons_lst):
        list_of_dicts = []
        for i in links_jsons_lst:
            date_publish = i.get('datePublish')
            guid = i.get('guid')
            #try:
            r = requests.get(f'https://fedresurs.ru/backend/sfactmessages/{guid}'
                    , headers = self.headers).json()
            if 'lessorsCompanies' in r['content'] or 'lessors' in r['content']:

                r['content']['lessors'] = r['content']['lessorsCompanies'] if 'lessorsCompanies' in r['content'] else r['content']['lessors']

                r['content']['subjects'] = [{**i, 'subjectId' : i['subjectId']}
                                            if 'subjectId' in i
                                            else {**i, 'subjectId' : i.get('identifier', '')} 
                                            for i in r['content']['subjects']
                                        ]

                r['content']['subjects'] = [{**i, 'classifierName' : i['classifierName']}
                                            if 'classifierName' in i
                                            else {**i, 'classifierName' : i['classifier']['description']} 
                                            for i in r['content']['subjects'] 
                                        ]

                lessor = r['content']['lessors'][0].get('fullName', '')
                lessor_inn = r['content']['lessors'][0].get('inn', '')
                lessor_ogrn = r['content']['lessors'][0].get('ogrn', '')
                lst = [{**i, 'lessor': lessor, "lessor_inn" : lessor_inn, "lessor_ogrn" : lessor_ogrn,
                                'guid' : guid, 'date_publish' : date_publish} 
                                for i in r['content']['subjects'] ]
                list_of_dicts += lst
            else:
                print("Wrong format of: ", guid)
                pass
            #except:
        return list_of_dicts

