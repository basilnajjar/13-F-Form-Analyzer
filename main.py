import os
import requests

from collections import OrderedDict
from operator import itemgetter
from bs4 import BeautifulSoup
from sec_edgar_downloader import Downloader
from retrieve_data import Download_FailsToDeliver_data, folder_path

CIKs = {'Renaissance Technologies LLC': '0001037389', 'TWO SIGMA INVESTMENTS, LP': '0001179392',
       'Third Point LLC': '0001040273', 'GREENLIGHT CAPITAL INC': '0001079114', 'Southeastern Asset Management': '000807985'}

# files that we're going to write the data into (in order to back-test on Quantopian)
desktop_path = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop')

DATA_DICT_FILE = desktop_path + '/13f data dictionary.txt'
REVISED_DATA_DICT_FILE = desktop_path + '/revised 13f data dictionary.txt'


class Scrape_13f():

    def __init__(self):
        self.dict = {}
        self.symbols_not_found = set()
        self.symbols = set()

    def get_links(self):
        dl = Downloader(desktop_path + "/13f filings")
        # get past 68 13f filings for each company
        # for c in CIKs.values():
        dl.get_13f_hr_filings('0000807985', 68)


    def scrape_shittyOld_13f(self, file):
        with open(file, encoding="utf8", errors='ignore') as f:
            src = f.read()

        dict = {}
        flag = False

        # clean the list
        row = src.replace('\n', '')
        row = row.split(' ')
        row = list(filter(None, row))
        row = list(filter(lambda a: a != ' 1', row))
        row = list(filter(lambda a: a != ' 2', row))

        renTec = False
        # check if the company is Renaissance Technologies because it needs special numerical treatment later on
        if '1037389' in file:
            renTec = True

        renewAtZero = flag2 = False
        # to avoid any conflicts, we will store the first integer number(shares value) and the value before that
        #  in each row (cusip)
        date = ''
        if row.count('0') > 10:
            renewAtZero = True

        for i in range(1, len(row)-2):
            if row[i] == 'AS' and row[i+1] == 'OF' and 'DATE' in row[i+2]:
                for c in row[i+2]:
                    if c.isdigit():
                        date += c

            # careful that we don't select the cusip
            if len(row[i]) != 9 and row[i].replace(',', '').isdigit() and not flag:
                # for some weird reason 2Sigma has the char ' attached to each cusip
                cusip = row[i-1].replace('\'', '')
                if len(cusip) != 9:
                    flag = False
                else:
                    value = row[i].replace(',', '')
                    if renTec:
                        if len(value) > 8:
                            value = value[:-8]
                            flag2 = True
                        if not row[i + 2].strip() == 'SH' and not flag2:
                            # SHUT THE FUCK UP
                            flag2 = False
                        else:
                            value = int(value)
                            flag = True
                            flag2 = False
                            # { CUSIP = VALUE (x1000) }
                            if cusip not in dict.keys():
                                dict[cusip] = value
                            else:
                                dict[cusip] = dict.get(cusip) + value

                    else:
                        value = int(value)
                        flag = True
                        flag2 = False
                        # { CUSIP = VALUE (x1000) }
                        if cusip not in dict.keys():
                            dict[cusip] = value
                        else:
                            dict[cusip] = dict.get(cusip) + value

            # if we don't have zeros
            if not renewAtZero and row[i] == 'SH':
                flag = False
            if renewAtZero and row[i] == '0':
                flag = False

        top5_invested_sec = sorted(dict.items(), key=itemgetter(1))[-5:]
        if date and top5_invested_sec:
            self.dict[date] = top5_invested_sec


    def scrape_13f(self, file):
        with open(file, encoding="utf8", errors='ignore') as f:
            src = f.read()
        soup = BeautifulSoup(src, 'xml')
        rows = soup.find_all('infoTable')

        dict = {}

        # this is not an xml file so we need to handle it differently
        if not rows:
            self.scrape_shittyOld_13f(file)
            return

        for row in rows:
            cusip = row.find('cusip')
            value = row.find('value')
            # { CUSIP = VALUE (x1000) }
            if cusip.text and value.text:
                dict[cusip.text] = int(value.text)

        top5_invested_sec = sorted(dict.items(), key=itemgetter(1))[-5:]
        date = soup.find('signatureDate').text.split('-')
        # for consistency with the old 13f filings date format (%Y%m%d)
        date = date[2] + date[0] + date[1]
        if date and top5_invested_sec:
            self.dict[date] = top5_invested_sec


    def reset_dict(self):
        self.dict = {}


    def get_13f_data(self):
        return self.dict


    def cusip_to_symbol(self, cusip):
        folder = os.fsencode(folder_path)
        symbol = ''

        for file in os.listdir(folder)[1:]:
            file = os.fsdecode(file)
            with open(folder_path + file, encoding="utf8", errors='ignore') as f:
                for line in f:
                    if cusip.upper() in line:
                        line = line.split('|')
                        symbol = line[2]
                        # A ticker having a ZZZZ or XXXX suffix indicates a special event such as, for example, a spinoff or merger
                        if symbol.endswith('XXXX') or symbol.endswith('ZZZZ'):
                            symbol = symbol[:-4]
                        else:
                            return symbol
                    # Greenlight Capital entered the wrong cusip (letter O instead of 0)
                    elif cusip == '7032241O5':
                        return 'POG'
                    # RenTec entered the wrong cusip for SLM Corporation
                    elif cusip == '90390U102':
                        return 'SLM'

        return symbol

    def replace_cusip_with_symbol(self, dict_data):
        for date, sec_list in dict_data.items():
            updated_sec_list = []
            for cusip, value in sec_list:
                symbol = self.cusip_to_symbol(cusip)
                if symbol:
                    if symbol not in self.symbols:
                        self.symbols.add(symbol)
                    # convert the cusip to a symbol
                    updated_sec_list.append((symbol, value))
                else:
                    # save the cusips that weren't converted
                    updated_sec_list.append((cusip, value))
                    self.symbols_not_found.add(cusip)

            dict_data[date] = updated_sec_list


    def get_symbols_not_found(self):
        return self.symbols_not_found


    def get_symbols(self):
        return self.symbols


    def add_symbol_func(self, data_dict):
        with open(DATA_DICT_FILE, 'w') as f:
            f.write(data_dict)

        f1 = open(DATA_DICT_FILE, 'r')
        f2 = open(REVISED_DATA_DICT_FILE, 'w')

        revised_data_dict = ''
        for line in f1:
            revised_data_dict = line
            for symbol in self.symbols:
                revised_data_dict = revised_data_dict.replace("'" + symbol + "'", 'symbol("' + symbol + '")')

            revised_data_dict = revised_data_dict.replace("'", "")

        f2.write(revised_data_dict)
        f1.close()
        f2.close()





obj = Scrape_13f()

'''download 13f filings from the SEC website'''
# obj.get_links()

base_url = desktop_path + '/13f filings/sec_edgar_filings/'
data_dict = {}

for CIK in CIKs.values():
    obj.reset_dict()
    CIK = CIK[3:]
    files_paths = os.listdir(base_url + CIK + '/13F-HR')
    for i in range(len(files_paths)):
        files_paths[i] = base_url + CIK + '/13F-HR/' + files_paths[i]

    # scrape company's 13fs
    for file in files_paths:
        obj.scrape_13f(file)

    company_13fs = obj.get_13f_data()
    # sort files by filing date
    ordered_company_13fs = OrderedDict(sorted(company_13fs.items(), key=lambda t: t[0]))
    ''''download Fails-to-Deliver Data files from the SEC in order to find the stock symbols'''
    # download_FailsToDeliver = Download_FailsToDeliver_data()
    # download_FailsToDeliver.download_files()
    obj.replace_cusip_with_symbol(ordered_company_13fs)
    data_dict[CIK] = ordered_company_13fs


print(data_dict)
print()
print(obj.get_symbols_not_found())
