import scrapy
from items import Hello2Item
def verify_comment(verify_and_comment):
    if len(verify_and_comment) == 2:
        verify = 'Trip Verified'
        comment = verify_and_comment[1]
    else:
        verify = 'Not Verified'
        comment = verify_and_comment[0]
    return verify,comment
class QuoteSpider(scrapy.Spider):
    name = 'Hello2'
    base_url = "https://www.airlinequality.com/airline-reviews/british-airways"
    pages = 37
    page_size = 100
    start_urls = []
    for i in range(1,pages):
        url = f"{base_url}/page/{i}/?sortby=post_date%3ADesc&pagesize={page_size}"
        start_urls.append(url)
        
    def parse(self, response):
        informations = response.css('article[itemprop="review"]')
        items = Hello2Item()
        for information in informations:
            verify_and_comment = information.css('div[class="text_content "]::text').extract()
            verify,comment = verify_comment(verify_and_comment)
            header = information.css('h2.text_header::text').extract() 
            name_customer = information.css('span[itemprop="name"]::text').extract() 
            time = information.css('time[itemprop="datePublished"]::text').extract()  
            country =  information.css('h3[class="text_sub_header userStatusWrapper"]::text')[1::2].extract()
            verify_and_comment = information.css('div[class="text_content "]::text').extract()
            verify,comment = verify_comment(verify_and_comment)
            rating = information.css('span[itemprop="ratingValue"]::text').extract()
            review_value = information.css('td[class="review-value "]::text').extract()
            items["header"] = header
            items["name"] = name_customer
            items["country"] = country
            items["time"] = time
            items["rating"] = rating
            items["verify"] = verify
            items["review_value"] =review_value
            items["comment"] = comment
            yield items