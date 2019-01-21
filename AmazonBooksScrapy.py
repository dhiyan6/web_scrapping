import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.conf import settings
import time
from __builtin__ import str

class AmazonBooksScrapper(scrapy.Spider):
    # Your spider definition
    name = "AmazonBooksScrapper"
    allowed_domains = ["amazon.in"]
    requestList = []
    requestedCount = 0
    
    
    def start_requests(self):
        
        x = 0
        browseNodeList = ['1318158031', '1318053031']
        
        
        for bn in browseNodeList:
            x = x+ 1
            
            if x >0 and x <=2:
                self.browseNode = bn
                for i in range(1,76):
                
                    request = scrapy.Request("http://www.amazon.in/s/?rh=n%3A"+str(self.browseNode)+"%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=popularity-rank",callback=self.parse)                     
                    self.requestList.append(request)
                    request = scrapy.Request("http://www.amazon.in/s/?rh=n%3A"+str(self.browseNode)+"%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=price-asc-rank",callback=self.parse)
                    self.requestList.append(request)
                    request = scrapy.Request("http://www.amazon.in/s/?rh=n%3A"+str(self.browseNode)+"%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=price-desc-rank",callback=self.parse)
                    self.requestList.append(request)
                    request = scrapy.Request("http://www.amazon.in/s/?rh=n%3A"+str(self.browseNode)+"%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=review-rank",callback=self.parse)
                    self.requestList.append(request)
                    request = scrapy.Request("http://www.amazon.in/s/?rh=n%3A"+str(self.browseNode)+"%2Cp_n_availability%3A1318484031&page="+str(i)+"&sort=date-desc-rank",callback=self.parse)
                    self.requestList.append(request)        
        yield self.requestList[0]                
    
    
    def parse(self, response):
        
        if response and response.css('.s-result-item'):
            for li in response.css('.s-result-item'):
                for anchor in li.css('a'):
                    if anchor.xpath("h3/text()") and anchor.xpath("h3/text()")[0].extract( ).strip() in ["Paperback","Kindle Edition"]:
                        itemLink = str(anchor.xpath('@href')[0].extract()).strip()
                        if itemLink and len(itemLink.split('/')) >= 5 and len(itemLink.split('/')[5]) == 10:
                            with open(str(self.browseNode)+'AmazonScrapy.txt', 'a') as f:  # Just use 'w' mode in 3.x
                                f.write(itemLink.split('/')[5] + '\n')
        if self.requestedCount < len(self.requestList):
            self.requestedCount = self.requestedCount + 1
            yield self.requestList[self.requestedCount]        


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0 (compatible; MSIE 7.0; Windows NT 6.1)'
})
process.crawl(AmazonBooksScrapper)
process.start() # the script will block here until the crawling is finished
