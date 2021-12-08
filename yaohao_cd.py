import requests
from lxml import etree
import pdfplumber
import xlrd
import pymssql
from datetime import datetime,timedelta
from urllib import parse as par
import os

'''
@Time: 2021-10-20 15:27
@Author: tiankaixin
'''

def log(s:str):
    t = datetime.time()
    print(t, '--', s)

def del_repeat(item_lis:list) ->list:
    '''字典列表的去重'''
    return [dict(t) for t in set([tuple(d.items()) for d in item_lis])]

def download(item_lis:list, dir:str=''):
    '''下载excel,pdf'''
    if dir:
        if not os.path.exists(dir):
            os.mkdir(dir)
    else:
        if not os.path.exists('未指定'):
            os.mkdir('未指定')
        dir = '未指定'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0'}
    for item in item_lis:
        # print('正在下载',item['link'])
        res = requests.get(item['link'], headers=headers)
        with open(dir +r'/' + item['filename'], 'wb+') as f:
            f.write(res.content)
        # print(item['filename'])

def _parse_filename(filename):
    '''从文件名提取‘楼盘名’字段
    '''
    index = str.find(filename, '日') + 1
    filename = filename[index:-4]
    s = ''
    for i in range(2, len(filename)):
        if filename[i] in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
            s = filename[:i]
            break
        if filename[i] in ['第']:
            s = filename[:i]
            break
        if filename[i] in ['一', '二', '三', '四', '五', '六', '七', '八', '九', '第']:
            s = filename[:i]
            break
    filename = s.replace('项目', '')
    return filename


def _parse_order(filename):
    filename = str(filename)
    index = str.find(filename, '日') + 1
    s = filename[index:-4]
    s = s.split('登记购房')[0]
    if '家庭' in s:
        s = s[-4:].replace('家庭', '')
    if '货币化' in s:
        s = s.split('货币化')[0][-2:]
    elif '普通' in s:
        s = '普通'
    elif '刚需' in s:
        s = '刚需'
    elif '棚改' in s:
        s = '棚改'
    return s

def _parse_xls(filename):
    '''解析Excel'''
    item_lis = []
    table = xlrd.open_workbook(filename).sheets()[0]
    rows = table.nrows
    if rows <= 1:
        print(filename)
        return
    source_file = filename.name
    build = _parse_filename(source_file)
    room_order = _parse_order(source_file)
    for row in range(1, rows):
        item = {}
        item['datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item['source_file'], item['build'] ,item['room_order'] = source_file, build ,room_order
        item['rounds'] = table.cell_value(row, 0)
        item['room_select_order'] = table.cell_value(row, 1)
        item['notary_lottery_code'] = table.cell_value(row, 2)
        item_lis.append(item)
    return item_lis


def _parse_pdf(filename):
    '''解析PDF'''
    item_lis = []
    with pdfplumber.open(filename) as pdf:
        is_page0 = True
        for page in pdf.pages:
            table = page.extract_table()
            if is_page0:
                if len(table) <= 2:
                    print(filename)
                    return
                is_page0 = False
            source_file = filename.name
            build = _parse_filename(source_file)
            room_order = _parse_order(source_file)
            for tr in table[2:]:
                item = {}
                item['datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                item['source_file'], item['build'] ,item['room_order'] = source_file, build ,room_order
                if tr[0]:
                    item['rounds'] = tr[0]
                    x = tr[0]
                else:
                    item['rounds'] = x
                item['room_select_order'] = tr[1]
                item['notary_lottery_code'] = tr[2]
                item_lis.append(item)
    return item_lis


def insert_sql(conn, cursor, item_lis):
    sql = "insert into T_NEWHOUSE_NEWCITY_Chengdu_YAOHAO (sDatetime,sSource_file,sBuild,sRoom_order,sRounds,sRoom_select_order,sNotary_lottery_code) values (%s,%s,%s,%s,%s,%s,%s)"
    cursor.executemany(sql, item_lis)
    conn.commit()


def parse():
    from pathlib import Path
    conn = pymssql.connect(
        server='10.32.66.19',
        database='Fang_Spider',
        user='Fang_Spider_admin',
        password='m5k1lWDU')
    # 连接数据库
    # --------------test
    # conn = pymssql.connect(
    #     server='10.24.64.167',
    #     database='spider_test',
    #     user='spider_test_admin',
    #     password='sLzr5AC7')
    # ------------test end-----------------
    cursor = conn.cursor()
    # 解析Excel目录中的文件
    for excel in Path('excel').glob('*.xls'):
        if '2021年' in str(excel):
            pubmonth =int(str(excel).split('2021年')[1].split('月')[0])
            if pubmonth<=10 and pubmonth >=5:
                try:
                    item_lis = _parse_xls(excel)
                    item_lis = [tuple(i.values()) for i in item_lis]
                    insert_sql(conn, cursor, item_lis)
                except Exception as e:
                    print(excel)
                    print(e)
        else:
            continue

    print('开始解析pdf')
    # 解析pdf目录中独有的文件
    for pdf in Path('pdf').glob('*.pdf'):
        if '2021年' in str(pdf):
            pubmonth =int(str(pdf).split('2021年')[1].split('月')[0])
            if pubmonth<=10 and pubmonth >=5:
                try:
                    if not Path('excel/' + pdf.name[:-4] + '.xls').exists():
                        item_lis = _parse_pdf(pdf)
                        item_lis = [tuple(i.values()) for i in item_lis]
                        insert_sql(conn, cursor, item_lis)
                except Exception as e:
                    print(pdf)
                    print(e)
        else:
            continue
    # 关闭数据库连接
    conn.close()


def save2mssql(item_lis):
    '''数据入库
    '''
    item_lis = [tuple(i.values()) for i in item_lis]

    sql = "insert into T_NEWHOUSE_NEWCITY_Chengdu_YAOHAO (sDatetime,sSource_file,sBuild,sRounds,sRoom_select_order,sNotary_lottery_code) values (%s,%s,%s,%s,%s,%s)"
    # --------------test
    conn = pymssql.connect(
        server='10.24.64.167',
        database='spider_test',
        user='spider_test_admin',
        password='sLzr5AC7')
    # ------------test end-----------------
    cursor = conn.cursor()
    cursor.executemany(sql, item_lis)
    conn.commit()
    conn.close()


def save2baidupan(baidupan_lis):
    '''操作浏览器保存到百度网盘
    baidupan_lis        --> list   [{},{},{}]
    baidupan_lis[:]     --> dict   {'link':'','extract_code':''}
    '''
    from time import sleep
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    profile = webdriver.FirefoxProfile(r'C:\Users\admin\AppData\Roaming\Mozilla\Firefox\Profiles\jmvphqjo.dev-edition-default')  # 加载登陆信息
    options = webdriver.FirefoxOptions()
    options.set_preference('permissions.default.image', 2)  # 无图模式
    # options.add_argument('--headless') #无头模式
    driver = webdriver.Firefox(profile, firefox_binary=r"E:\FIREFOX\firefox.exe", options=options)
    for item in baidupan_lis:
        url = item['link']
        driver.get(url)
        # 链接被封
        if driver.find_elements_by_xpath('//*[@id="share_nofound_des"]') != []:
            with open('baidu/失效链接.txt', 'a', encoding='utf-8') as f:
                f.write('链接被封：  ' + item['link'] + '(' + item['extract_code'] + ')\n')
            continue
        # 提取码:求而莫得
        if (item['extract_code'] == '') and (driver.find_elements_by_xpath('//*[@id="accessCode"]') != []):
            with open('baidu/失效链接.txt', 'a', encoding='utf-8') as f:
                f.write('无提取码：  ' + item['link'] + '\n')
            continue
        # 提取码：求而有之
        if (item['extract_code'] != '') and (driver.find_elements_by_xpath('//*[@id="accessCode"]') != []):
            oInput = driver.find_element_by_xpath('//*[@id="accessCode"]')
            oInput.send_keys(item['extract_code'])
            oSubmit = driver.find_element_by_xpath('//*[@id="submitBtn"]//span')
            oSubmit.click()
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[text()="保存到网盘"]')))
            except:
                if driver.find_elements_by_xpath('//*[@class="tip" and text()="提取码错误"]') != []:  # 提取码错误
                    with open('baidu/失效链接.txt', 'a', encoding='utf-8') as f:
                        f.write('提取码错误：  ' + item['link'] + '(' + item['extract_code'] + ')\n')
                    continue
        # 提取码：都过去了
        driver.find_element_by_xpath('//*[text()="保存到网盘"]').click()
        sleep(1.4)
        try:
            # 点击 目录“成都公证处”
            oCdgzc = driver.find_element_by_xpath('//*[@class="treeview-txt" and text()="成都公证处"]')
            driver.execute_script('arguments[0].scrollIntoView();', oCdgzc)
            oCdgzc.click()
        except:
            # 勾选 “最近保存路径”
            driver.find_element_by_xpath('//*[@class="save-chk-io"]').click()
        driver.find_element_by_xpath('//*[@class="text" and text()="确定"]').click()
    driver.quit()

def extract(element, xpath_str):
    x = element.xpath(xpath_str)
    if x == []:
        return ''
    return x[0].strip()

def move_file():
    '''百度网盘下载的文件移到指定目录
    这样写多此一举，体验一下 for i,j in zip() 语法。
    更好的写法： for pdf in   ，  for excel in 
    '''
    from shutil import move
    from pathlib import Path
    # Excel，pdf  1：1移动
    for pdf, excel in zip(Path('baidu/成都公证处').glob('*.pdf'), Path('baidu/成都公证处').glob('*.xls')):
        pdf_path = 'pdf' + str(pdf)[11:]
        excel_path = 'excel' + str(excel)[11:]
        move(pdf, pdf_path)
        move(excel, excel_path)
    # pdf文件数量多于Excel文件
    for pdf in Path('baidu/成都公证处').glob('*.pdf'):
        pdf_path = 'pdf' + str(pdf)[11:]
        move(pdf, pdf_path)


def spider1(day = 7) ->tuple:
    '''爬下载链接'''
    xls_lis = []
    pdf_lis = []
    baidupan_lis = []
    url = 'http://www.cdgzc.com/gongshigonggao/'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0'}
    for i in range(1, 1000):  #列表页 --> 后续列表页
        htm = etree.HTML(requests.get(url, headers=headers).text)
        if url == 'http://www.cdgzc.com' + htm.xpath('//div[@id="page"]/a[last()-1]/@href')[0].strip():
            break  # 页数循环终止
        print('第', i, '页')
        urls = htm.xpath('//a[@class="title"]/@href')
        for url in urls:  # 遍历当前列表页的详情页
            url = 'http://www.cdgzc.com' + url
            html = etree.HTML(requests.get(url, headers=headers).text)
            # 爬七天的
            public_time =  extract(html, '//p[@class="date"]/text()').replace('发布日期', '').replace('：', '').replace(':', '').strip()
            





            a_lis = html.xpath('//div[@class="intro"]//a')
            for a in a_lis:
                _ = a.xpath('./@href')
                link = _[0].strip() if _ !=[] else ''
                if not link:
                    continue
                item = {}
                if '.xls' in link:
                    item['link'] = 'http://www.cdgzc.com' + link if 'http' not in link else link
                    item['filename'] = a.xpath('.//text()')[0].strip()
                    xls_lis.append(item)
                elif '.pdf' in link:
                    item['link'] = 'http://www.cdgzc.com' + link if 'http' not in link else link
                    item['filename'] = a.xpath('.//text()')[0].strip()
                    pdf_lis.append(item)
                elif 'pan.baidu' in link:
                    if 'http://www.cdgzc.com/' in link:
                        item['link'] = link.replace('http://www.cdgzc.com/', '').strip()
                    item['link'] = link
                    try:
                        string = a.xpath('./ancestor::p')[0].xpath('string(.)').replace('http://', '')
                        a = string.split('提取码:')
                        b = string.split('提取码：')
                        item['extract_code'] = a[1].strip() if a != [] else b[1].strip()
                    except Exception as e:
                        print(e)
                        item['extract_code'] = ''
                    if len(item['extract_code']) != 4:
                        print(item)
                    baidupan_lis.append(item)
        url = 'http://www.cdgzc.com' + htm.xpath('//div[@id="page"]/a[last()-1]/@href')[0].strip()
        # break
    del_repeat(xls_lis)
    del_repeat(pdf_lis)
    del_repeat(baidupan_lis)
    return xls_lis, pdf_lis, baidupan_lis

def spider2() ->tuple:
    xls_lis = []
    pdf_lis = []
    url = 'http://www.scgzc.com/newslist.aspx?classid=16&type=&page=1'
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0',
    'Host': 'www.scgzc.com'
    }
    is_step = False
    for page in range(1,1000):
        print('第',page,'页')
        htm = etree.HTML(requests.get(url, headers=headers).text)
        li_lis = htm.xpath('//div[@class="cftuiuij_right1"]/ul/li')
        for li in li_lis:
            pubmonth = int(li.xpath('./span/text()')[0].split('-')[1].split('-')[0])
            print('当前月',pubmonth)
            if pubmonth >11 or pubmonth<5:
                is_step = True
                break
            url = "http://www.scgzc.com" +li.xpath('./a/@href')[0].strip()
            html = etree.HTML(requests.get(url, headers=headers).text)
            a_lis = html.xpath('//a[@target="_blank"]')
            for a in a_lis:
                try:
                    link = a.xpath('./@href')[0].strip()
                    link = link.replace('http://','',1) if link.count('http://') ==2 else link
                except Exception as e:
                    link = ""
                    print(url)
                    print(e)
                if '.xls' in link:
                    print("excel:",link)
                    item = {}
                    item['link'] = link
                    item['filename'] = par.unquote(a.xpath('.//text()')[0].split('/')[-1].split('.')[0]) + '.xls'
                    xls_lis.append(item)
                if '.pdf' in link:
                    print("pdf:",link)
                    item = {}
                    item['link'] = link
                    item['filename'] = par.unquote(a.xpath('.//text()')[0].split('/')[-1].split('.')[0]) + '.pdf'
                    pdf_lis.append(item)
        if is_step:
            break
        try:
            url = "http://www.scgzc.com/" + htm.xpath('//a[text()="下页"]/@href')[0].strip()
        except:
            break
    del_repeat(xls_lis)
    del_repeat(pdf_lis)
    return xls_lis,pdf_lis
     

def mkdir(dir):
    if not os.path.exists(dir):
        os.mkdir(dir)
        
def reinit():
    '''七天'''
    mkdir('document')
    mkdir('document/pdf')
    mkdir('document/excel')
    before7 = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    print(before7)
    xls_lis, pdf_lis, baidupan_lis = spider1()
    xls_lis, pdf_lis = spider2()

    



if __name__ == '__main__':
    reinit()
    # print('获取下载链接······')
    # xls_lis, pdf_lis, baidupan_lis = spider()
    # print('下载Excel中······')
    # download(xls_lis, path='excel')
    # print('下载pdf中······')
    # download(pdf_lis, path='pdf')

    # import json
    # # with open('baidu/原本.txt', 'w', encoding='utf-8') as f:
    # #     f.write(json.dumps(baidupan_lis))
    # with open('baidu/运行链接.txt', 'r', encoding='utf-8') as f:
    #     baidupan_lis = json.loads(f.read())
    # print(len(baidupan_lis), '个')
    # # # 读取某个链接位置
    # # for i in range(len(baidupan_lis)):
    # #     if baidupan_lis[i]['link'] == 'https://pan.baidu.com/s/10BGM8-8-GuZcPxM_oiWElA':
    # #         print(i)
    # save2baidupan(baidupan_lis)

    # move_file()

    # parse()


    # # 5-10月
    # # 1.读取目录,若有新增，下载
    # xls_lis,pdf_lis = spider2()
    # # print(xls_lis)
    # # print(pdf_lis)
    # download(xls_lis,path='excel/')
    # download(pdf_lis,path='pdf/')





