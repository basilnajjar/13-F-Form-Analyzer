from urllib import request
from zipfile import ZipFile
from bs4 import BeautifulSoup

import os
import requests

desktop_path = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop')

# we need the fails to deliver data files (released each quarter by the SEC) in order to find the stock symbols since
# the 13F forms only contain the CUSIP numbers
fails_to_deliver_data_url = 'https://www.sec.gov/data/foiadocsfailsdatahtm'
folder_path = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop') + '/Fails-to-Deliver Data/'
base_SEC_url = 'https://www.sec.gov'


class Download_FailsToDeliver_data():
    def download_files(self):
        src = requests.get(fails_to_deliver_data_url)
        if src.status_code == 200:
            soup = BeautifulSoup(src.content, 'lxml')
            table = soup.find_all('table')[1]

            download_links = table.find_all('a')

            for link in download_links:
                if link.text and link.attrs['href']:
                    file_name = link.text
                    file = folder_path + file_name + '.zip'
                    request.urlretrieve(base_SEC_url + link.attrs['href'], file)

                    with ZipFile(file, 'r') as zip:
                        # extracting file
                        zip.extractall(folder_path)
                        # remove original zip file
                        os.remove(file)
                        # os.rename(folder_path + original_file_name + '.txt', folder_path + file_name + '.txt')
