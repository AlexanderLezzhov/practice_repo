import pandas as pd
import numpy as np
import json
import re

class Dataframe_prep():
    def __init__(self):
        pass
       #self.messege_subjects_dict = messege_subjects_dict

    def df_creator(self, messege_subjects_dict):

        is_vin = lambda x: len(x) == 17 and x[-4:].isdigit() #У VIN-кода длина всегда 17 символов и последние 4 - цифры

        try:
            df = pd.DataFrame.from_dict(messege_subjects_dict)
            df = df[['guid', 'lessor', 'lessor_inn', 'lessor_ogrn', 'subjectId', 'classifierName', 'description', 'date_publish']]
            df.columns = ['guid', 'lessors', 'lessors_inn', 'lessors_ogrn', 'identifier', 'classifier', 'description', 'datePublish']

            df['description_split'] = df['description'].apply(lambda x: re.split(',|:| |\n|\t', str(x) ) )
            df['description_split'] = df['description_split'].apply(lambda x: list(np.concatenate([ [i[:17], i[17:]]  if  len(i)==34 else [i] for i in x ] )))

            df['subjectId_split'] = df['identifier'].apply(lambda x: re.split(',|:| |\n|\t', x ) )
            df['subjectId_split'] = df['subjectId_split'].apply(lambda x: list(np.concatenate([ [i[:17], i[17:]]  if  len(i)==34 else [i] for i in x ] )))

            df['vins_from_description'] = df['description_split'].apply(lambda x: [i for i in x if is_vin(i) ])
            df['vins_from_subjectId'] = df['subjectId_split'].apply(lambda x: [i for i in x if is_vin(i) ])

            df['vin_id_corrected'] = df.apply(lambda x: list(set(x.vins_from_description + x.vins_from_subjectId) ), axis =1 )

            df = df.explode('vin_id_corrected')
            df = df[['guid', 'lessors', 'lessors_inn', 'lessors_ogrn', 'identifier', 'classifier', 'description', 'vin_id_corrected','datePublish']]
            return df
        
        except:
            print('Wrong dict format')