from bs4 import BeautifulSoup
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from datetime import datetime
import json
import pandas
import re
import os.path

#API SCRAPE CMC DATABASE FOR THE TOP (#REPETITION) CRYPTOCURRENCIES, EXTRACTING GENERAL, TAG, QUOTE AND PLATFORM DATA
def cryptoScrape(repetition, generalData, cryptoTagData, tagData, quoteData, platformData):
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
    'start':'1',
    'limit': repetition,
    'convert':'AUD'
    }
    
    headers = {
        'Accepts': 'application/json',
        'YOUR API KEY HERE'
        }
    
    session = Session()
    session.headers.update(headers)
    
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        print('SUCCESS')   
        print("SCAN TIME: " + str(datetime.now()))

        cryptoTagId = 0  
        tagId = 0
        cryptoQuoteId = 0      
        for crypto in data['data']:
            
            # CRYPTO_TAG DATA EXTRACTION            
            for cryptoTag in crypto['tags']:
                cryptoTagItem = {}
                cryptoTagItem['CRYPTO_TAG_ID'] = cryptoTagId
                cryptoTagItem['crypto_id'] = crypto['id']
                cryptoTagItem['tag_id'] = cryptoTag
                cryptoTagData.append(cryptoTagItem)   
                cryptoTagId+=1 
                
                # TAG DATA EXTRACTION            
                tagItem = {}
                tagItem['TAG_ID'] = None
                tagItem['tag'] = cryptoTag
                tagData.append(tagItem)   
                tagId+=1                 
            del crypto['tags']                                  
            
            # CRYPTO_QUOTE DATA EXTRACTION
            quoteItem = {}
            quoteItem['CRYPTO_QUOTE_ID'] = cryptoQuoteId
            quoteItem['crypto_id'] = crypto['id']
            quote = crypto['quote']['AUD']
            del quote['last_updated']
            quoteItem.update(quote)
            quoteData.append(quoteItem)
            cryptoQuoteId += 1
            del crypto['quote']
    
            # CRYPTO_PLATFORM DATA EXTRACTION
            platform = crypto['platform']
            if platform != None:            
                platformItem = {}
                platformItem['CRYPTO_PLATFORM_ID'] = platform['id']
                del platform['id']
                del platform['token_address']
                platformItem.update(platform)
                platformData.append(platformItem)
                crypto['platform'] = platformItem['CRYPTO_PLATFORM_ID']
                      
            
            # CRYPTO_GENERAL DATA EXTRACTION
            cryptoItem = {}
            cryptoItem["CRYPTO_ID"] = crypto['id']
            cryptoItem['cmc_rank'] = crypto['cmc_rank']
            cryptoItem['crypto_platform_id'] = crypto['platform'] 
            date_time_added = crypto['date_added'][:10] + ' ' + crypto['date_added'][11:19]
            cryptoItem['date_time_added'] = date_time_added
            del crypto['date_added']    
            del crypto['platform']
            del crypto['id']
            del crypto['cmc_rank']
            del crypto['self_reported_circulating_supply']
            del crypto['self_reported_market_cap']
            del crypto['last_updated']
            cryptoItem.update(crypto)         
            generalData.append(cryptoItem)
            
        # FILTER PLATFORM FOR DISTINCT VALUES ONLY
        platformDataDistinct = list({v['CRYPTO_PLATFORM_ID']:v for v in platformData}.values())
        platformData[:] = platformDataDistinct
        
        # FILTER TAG FOR DISTINCT VALUES ONLY
        tagDataDistinct = list({v['tag']:v for v in tagData}.values())
        tagData[:] = tagDataDistinct
        
        # ASSIGN PRIMARY KEY TO TAG_DATA
        for i in range(0, len(tagData)):
            tagData[i]["TAG_ID"] = i
        
        # SEARCH FOR TAG_ID ASSIGNED, THEN ASSIGN AS FOREIGN KEY TO CRYPTO TAG                                
        for i in range(0, len(cryptoTagData)):
            tag_id = next(item for item in tagData if item["tag"] == cryptoTagData[i]["tag_id"])['TAG_ID']
            cryptoTagData[i]['tag_id'] = tag_id        
        
    except (ConnectionError, Timeout, TooManyRedirects) as e:   
        print('ERROR')
        print(e)

#EXPORT DATA TO EXCEL
def export_data(data, name, existingFile):
    df = pandas.DataFrame(data)       
    
    if existingFile == False:
        with pandas.ExcelWriter('coinMarketCap.xlsx') as writer: 
            df.to_excel(writer, sheet_name = name, index=False)
    else:
        with pandas.ExcelWriter('coinMarketCap.xlsx', engine='openpyxl', mode='a') as writer:  
            df.to_excel(writer, sheet_name= name, index=False)   
    
#ACTIVATE COMMAND FOR SCRAPING "REPETITION" AMOUNT OF DATA POINTS
def activateScrapeTimesList(repetition):
    generalData = []
    cryptoTagData = []
    tagData = []
    quoteData = []
    platformData = []
    cryptoScrape(repetition, generalData, cryptoTagData, tagData, quoteData, platformData)
    existingFile = False
    
    #EXPORT CRYPTO DATA
    export_data(generalData, "crypto",existingFile)
    existingFile = True    
    
    #EXPORT CRYPTO_TAG DATA
    export_data(cryptoTagData, "cryptoTag",existingFile)
    
    #EXPORT TAG DATA
    export_data(tagData, "tag",existingFile)
    
    #EXPORT CRYPTO_QUOTE DATA
    export_data(quoteData, "cryptoQuote",existingFile)
    
    #EXPORT CRYPTO_PLATFORM DATA
    export_data(platformData, "cryptoPlatform",existingFile)
    
    print("TOTAL DATA COLLECTION: " + str(len(generalData)))

### EXECUTE
activateScrapeTimesList(100)