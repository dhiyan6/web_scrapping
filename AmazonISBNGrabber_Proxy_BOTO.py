
from lxml import html
import requests
import time
from __builtin__ import str
import collections
import csv
import boto3
import traceback
import sys
import random
import pdb
import codecs

class AmazonBooksScrapper():

    requestList = []
    currentURL = ""
    lastTenResponseStatus = [1,1,1,1,1,1,1,1,1,1]
    lastTenResponseStatusPointer = 0
    lastTenResponseStatusSum = 10
    scrapedCount = 0
    proxyDict = {"http":"35.154.18.241:888"}
    proxiesList = []
    ipRotatingCount = 0
    isbnCount = 0
    browseNodes = ['1318158031', '1318053031']

    def isIPRotateNeededed(self,responseStatus):
        
        self.lastTenResponseStatusSum = self.lastTenResponseStatusSum -self.lastTenResponseStatus[self.lastTenResponseStatusPointer] + responseStatus
        self.lastTenResponseStatus[self.lastTenResponseStatusPointer] = responseStatus
        self.lastTenResponseStatusPointer = (self.lastTenResponseStatusPointer + 1)%10
        if self.lastTenResponseStatusSum <= 8:
            return True
        else:
            return False
        
    def rotateIP(self):
        self.lastTenResponseStatusSum = 10
        self.lastTenResponseStatus = [1,1,1,1,1,1,1,1,1,1]
        self.lastTenResponseStatusPointer = 0
        self.ipRotatingCount = self.ipRotatingCount + 1
        print "Rotating Proxy"+str(self.proxyDict)

        client = boto3.client(
            'ec2',
                aws_access_key_id='*****************',
                aws_secret_access_key='***********************8',
                region_name ='ap-south-1',
            )

        client.stop_instances(InstanceIds=['i-0c384ef6653f6fd41'])
        time.sleep(120)
        client.start_instances(InstanceIds=['i-0c384ef6653f6fd41'])
        time.sleep(120)
        instancePropertyDict = client.describe_instances(InstanceIds=['i-0c384ef6653f6fd41'])
        self.proxyDict['http'] = instancePropertyDict['Reservations'][0]['Instances'][0]['PublicIpAddress']+":888"
    
    def start_requests(self):
        for j in range(0,50):
            priceRange = [{'low':0,'high':250},{'low':251,'high':500},{'low':501,'high':750},{'low':751,'high':1000},{'low':1001,'high':1250},\
                          {'low':1251,'high':1500},{'low':1501,'high':1750},{'low':1751,'high':2000},{'low':2001,'high':3000},{'low':3001,'high':4000},\
                          {'low':4001,'high':5000},{'low':5001,'high':1000000}]
            self.browseNode = self.browseNodes[j]
            for i in range(1,21):
                for price in priceRange:
                    request = "http://www.amazon.in/s/?rh=n%3A"+str(browseNode)+"%2Cp_n_binding_browse-bin%3A1318376031%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=popularity-rank&lo=stripbooks&low-price="+str(price['low'])+"&high-price="+str(price['high'])                     
                    self.requestList.append(request)
                    request = "http://www.amazon.in/s/?rh=n%3A"+str(browseNode)+"%2Cp_n_binding_browse-bin%3A1318376031%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=price-asc-rank&lo=stripbooks&low-price="+str(price['low'])+"&high-price="+str(price['high'])
                    self.requestList.append(request)
                    request = "http://www.amazon.in/s/?rh=n%3A"+str(browseNode)+"%2Cp_n_binding_browse-bin%3A1318376031%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=price-desc-rank&lo=stripbooks&low-price="+str(price['low'])+"&high-price="+str(price['high'])
                    self.requestList.append(request)
                    request = "http://www.amazon.in/s/?rh=n%3A"+str(browseNode)+"%2Cp_n_binding_browse-bin%3A1318376031%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=review-rank&lo=stripbooks&low-price="+str(price['low'])+"&high-price="+str(price['high'])
                    self.requestList.append(request)
                    request = "http://www.amazon.in/s/?rh=n%3A"+str(browseNode)+"%2Cp_n_binding_browse-bin%3A1318376031%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=date-desc-rank&lo=stripbooks&low-price="+str(price['low'])+"&high-price="+str(price['high'])
                    self.requestList.append(request)
        
    
    def parse(self,response):
            item = {}
            count = 0
            try:
                if response.xpath('//*[@id="s-results-list-atf"]'):
                    for li in response.xpath('//a[@class = "a-link-normal s-access-detail-page  a-text-normal"]'):
                        if li.xpath('@title'):
                            self.isbnCount += 1
                            count = count + 1
                            
                            print len(self.requestList),self.scrapedCount,self.isbnCount,li.xpath('@title')[0],li.xpath('@href')[0]
                            with codecs.open(str(self.browseNode)+'.txt', 'a',encoding="utf-8") as f:
                                f.write(li.xpath('@title')[0])
                                f.write(',')
                                f.write(li.xpath('@href')[0])
                                f.write("\n")
                        
                elif response.xpath('//*[@id="noResultsTitle"]'):
                    with open('logger.txt', 'a') as f:
                        f.write("Moving to Next Price Range")
                        f.write(str(self.requestList[self.scrapedCount - 1]))
                        
                    self.scrapedCount = (((self.scrapedCount - 1) / 5) + 1)*5 
                else:
                    self.requestList.append(self.currentURL)    
                    if self.isIPRotateNeededed(0):
                        self.rotateIP()
                    
            except Exception,e:            
                    with open('logger.txt', 'a') as f:
                        f.write(str(self.requestList[self.scrapedCount - 1]))
                        f.write(str("\nError : "+str(e) + '\t' +str(traceback.format_exc())))
                            
if __name__ == "__main__":
    
    amazonBooksScrapperOBJ = AmazonBooksScrapper()
    amazonBooksScrapperOBJ.start_requests()
    
                        
    #for eachLink in abs.requestList:
    headers = {
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding":"gzip, deflate, sdch",
    "Accept-Language":"en-US,en;q=0.8",
    "Avail-Dictionary":"kGp3MO8s",
    "Cache-Control":"max-age=0",
    "Connection":"keep-alive",
    "Host":"www.amazon.in",
    "Upgrade-Insecure-Requests":"1",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
    "Referer":"",
    "Cookie":"x-wl-uid=1ZrecvTFWmMerG7DDZa5B012JqaKNekAX6ZbKgVnomqb5yQYFgVmJj1YvEZjC6fVKXeN5Cb29I04=; session-token=o9MLbKjvM6EvJjFJpUsMZg50zVkzSaj9lIZM2nZzzCWQs1Z6OtTarof41uzZzbGYE59eIhomcWDyd4++gIPCESehbotpe/gmD6RtymUQc/nXKcxF/wHhifZy2wXxLmdBOHci8FNSst2T4KgAe4aOperX5PI5crn4+xtakRyixq7eSIUbFv2HktWufBVhnmlJ6ZfvoZt6CqBuDc/TSVpf/pfZn87j88z5acwpu1XxEtOl4H6e/Xhnsw==; visitCount=2; csm-hit=0TM3N076DYA20EWXWP2E+s-1WYZJV1GBA5FT6DN0N5W|1467961147266; lc-acbin=en_IN; ubid-acbin=277-8812767-2995748; session-id-time=2082758401l; session-id=277-2038617-4047358"
    }
    
    
    print amazonBooksScrapperOBJ.requestList
    while amazonBooksScrapperOBJ.scrapedCount < len(amazonBooksScrapperOBJ.requestList):
        amazonBooksScrapperOBJ.currentURL = amazonBooksScrapperOBJ.requestList[amazonBooksScrapperOBJ.scrapedCount]
        amazonBooksScrapperOBJ.scrapedCount = amazonBooksScrapperOBJ.scrapedCount + 1 
        headers["Referer"] = amazonBooksScrapperOBJ.currentURL
        try:
            page = requests.request("GET", amazonBooksScrapperOBJ.currentURL ,headers = headers,proxies = amazonBooksScrapperOBJ.proxyDict,timeout = 5)
            #print page.content
            amazonBooksScrapperOBJ.parse(html.fromstring(page.content))                                               
    
        except Exception,e:
            amazonBooksScrapperOBJ.rotateIP()
            amazonBooksScrapperOBJ.requestList.append(amazonBooksScrapperOBJ.currentURL)