import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class LezzhovCustomRAMOperator(BaseOperator):

    template_fields = ('rows_count',)

    def __init__(self, rows_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.rows_count = rows_count

    def get_vals_from_ram_api(self, row_count) -> str:
        api_data = requests.get(f"https://rickandmortyapi.com/api/location")
        if api_data.status_code == 200:
            data = api_data.json()
            logging.info('Connection OK')
            results_filt = sorted(data['results'], key=lambda x: len(x['residents']), reverse=True)[:3]
            results_res_cnt = [str((x['id'], x['name'], x['type'], x['dimension'], len(x['residents']))) for x in results_filt]
            vals_for_insert = ','.join(results_res_cnt)
            return vals_for_insert
        else:
            logging.warning("HTTP STATUS {}".format(api_data.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context, **kwargs):
        return self.get_vals_from_ram_api(self.rows_count)
        logging.info(r'values collected')