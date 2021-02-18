import requests
import bs4
from bs4 import BeautifulSoup
import csv
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import re

links = []

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Accept-Language': 'en-US,en;q=0.9',
}

with open('Schule Suchen _ Suchergebnis.html', 'r', encoding='utf-8') as newfile:
    name = newfile.read()
    new = BeautifulSoup(name,'lxml')
    for i in new.find_all('a', class_ = "links fl"):
        links.append(i.attrs['href'])
count = [i for i in range(len(links))]
def utilfunc(self):
    self = re.sub("\s\s+", " ", self)
    return self
def newfunc(link,count1):
    x = True
    while x:
        try:
            place = requests.get(link, headers=headers).content
            if place:
                x = False
        except:
            continue
    
    soup = BeautifulSoup(place, 'lxml').find_all('div', class_ = "ui-g")[1]

    # with open('bleh.html','w') as newfile:
    #     newfile.write(soup.prettify())
    
    first = soup.find_all('div')[1].table.tbody.find_all("tr")
    

    schoolNo = utilfunc(first[0].text.split(' ')[1].strip())
    info = utilfunc(first[1].text.strip())
    public = utilfunc(first[3].text.strip())
    street = utilfunc(first[5].text.strip())
    post = utilfunc(first[6].text.strip())
    telephone = utilfunc(first[8].text.strip())
    fax = utilfunc(first[9].text.strip())
    try:
        email = utilfunc(first[11].text.strip())
        if "http" in email:
            email = utilfunc(first[10].text.strip())
    except:
        email = utilfunc(first[10].text.strip())
    try:
        site = utilfunc(first[12].text.strip())
    except:
        site = ""
        
        
    print(count1+1)
    
    second = soup.find_all('div')[2].find_all('li')
    

    schoolType = utilfunc(second[0].text.strip())
    hasOperations = utilfunc(second[1].text.strip())
    studentNo = utilfunc(second[2].text.strip())
    moreInfo = ""
    if len(second)>3:
        for m,i in enumerate(second[3:]):
            moreInfo = moreInfo+ utilfunc(second[m+3].text.strip())
    x = soup.find_all('div')
    furtherInfo = ""
    try:
        ps = BeautifulSoup(place, 'lxml')
    
        for i in ps.find_all("div", class_ = "ui-g-12 ui-md-12 ui-lg-4 dataColumn")[2].table.tbody.find_all("tr"):
            furtherInfo= furtherInfo+" "+i.text.strip()+"\n"
        furtherInfo = furtherInfo.strip()
    
    except:
        furtherInfo = ""
    classes1 = ""
    try:
        pe = BeautifulSoup(place, 'lxml')
    
        for i in pe.find("div", class_ = "ui-g-12 ui-md-12 ui-lg-12 dataColumn").ul.findChildren():
            classes1= classes1+" "+i.text.strip()+"\n"
        classes1 = classes1.strip()
    
    except:
        classes1 = ""
    # print([schoolNo,info,public,street,post,telephone,fax,email,site])
    # print([schoolType,hasOperations,studentNo,specialInfo,reform])
    # for i,each in enumerate(second):
    #     print(f"{i} - {utilfunc(each.text.strip())}")
    return [link,schoolNo,info,public,street,post,telephone,fax,email,site,schoolType, hasOperations,moreInfo,furtherInfo,classes1]
# newfunc("https://www.schulministerium.nrw.de/BiPo/SchuleSuchen/pages/schulsuche/schule_information_seite.xhtml?schulnummer=100127", 1)   
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(newfunc, links[5124:],count[5124:])
    
    for result in results:
        with open('news.csv', 'a', encoding= 'utf-8', newline = '') as newfile1:
            writer = csv.writer(newfile1)
            writer.writerow(result)
    