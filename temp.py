from settings import hospital_data
from utr_search_backend import gmail_apiv2

hosp = 'mediclaimnoble'
gmail_apiv2(hospital_data[hosp], hosp, '', 'text', '01/01/2021 11:11:11')