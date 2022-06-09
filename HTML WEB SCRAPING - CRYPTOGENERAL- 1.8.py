from bs4 import BeautifulSoup
import requests
import pandas
import re

def listTop100():
    html_text = requests.get('https://coinmarketcap.com/').text
    soup = BeautifulSoup(html_text,'lxml')
    cryptoCurrencies = soup.find_all('tr') 
    linkList = []    

    for crypto in cryptoCurrencies:
        cryptoInfo = crypto.find_all("a")    
        
        if len(cryptoInfo) != 0:
            linkList.append(cryptoInfo[0]['href'])     

    return linkList        

def cryptoScrape(generalData, cycle):
    rankingList = []    
    for cryptoLink in listTop100():
        item = {}
        html_text = requests.get('https://coinmarketcap.com'+cryptoLink).text
        soup = BeautifulSoup(html_text,'lxml')              

        item['RANK'] = soup.find('div', class_='namePill namePillPrimary').text[6:] 
        item['SYMBOL'] = soup.find('small', class_='nameSymbol').text        
        item['NAME'] = soup.find('h2', class_='sc-1q9q90x-0 jCInrl h1').text[:-len(item['SYMBOL'])]
        item['PRICE'] = soup.find('div', class_='priceValue').text
        item['MARKETCAP'] = soup.find_all('div', class_='statsValue')[0].text
        item['MARKETCAP_DILUTED'] = soup.find_all('div', class_='statsValue')[1].text 
        item['VOLUME_24HR'] = soup.find_all('div', class_='statsValue')[2].text 
        item['VOLUME/MARKETCAP'] = soup.find_all('div', class_='statsValue')[3].text         
        item['CIR_SUPPLY'] = soup.find_all('div', class_='statsValue')[4].text
        rankingList.append(item['RANK'])
        #print(item['RANK'])                  
        item['RECENT ARTICLES'] = soup.find('div', class_='sc-101ku0o-2 exKUGw').text      
        generalData.append(item)
        print(item) 
                              
def activateScrapeTimesList(repetition, generalData):
    for i in range(1,repetition+1):
        cryptoScrape(generalData, i)  
    print("TOTAL DATA COLLECTION: " + str(len(generalData)))

def export_data(data, name):
    df = pandas.DataFrame(data)
    df.to_excel(name + ".xlsx")
    df.to_csv(name + ".csv")

generalData = []
tagData = []

activateScrapeTimesList(1, generalData)
export_data(generalData, "cryptoGeneral")