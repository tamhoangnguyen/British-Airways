# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
import re
def tranform(data):
    data['header'] = re.sub(r'[",\r,\n]', "", data['header'][0]).replace(",",";")
    data['name'] = data['name'][0] 
    data['country'] = data['country'][0].replace("(","").replace(")","").strip()
    data['time'] = data['time'][0]
    data['rating'] = int(data['rating'][0])
    data['comment'] = re.sub(r'[\r,\n]', "", data['comment']) 
    return data
result = []
class Hello2Pipeline:
    def process_item(self, item, spider):
        data = dict(item)
        data = tranform(data)
        result.append(data)
        if len(result) >= 3590 and len(result) <= 3600:
            df = pd.DataFrame(result)
            df.to_csv("D:\My_project\Airport_Project_Batch_Processing\data.csv")
            print(f'Load {len(result)} was success !')
        

        
            