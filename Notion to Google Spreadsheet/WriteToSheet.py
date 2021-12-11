import os
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

class Integration():
    def __init__(self):
        self.api_key = "secret_faCbeR4NUmRpK4gFi7QwrypIZeYesPt0OaM9dnMb1yU"
        self.database_id = "0b925229bcfa419d80f519485066fd2b"
        self.scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('eloquent-victor-333708-b2b68b2fd36b.json', self.scope)
        self.client = gspread.authorize(self.creds)
        self.sheet = self.client.open('Testing').worksheet('Sheet1')
    
    def load_page(self):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Notion-Version": "2021-08-16",
            "Content-Type": "application/json",
            }
        response = requests.post(
            f"https://api.notion.com/v1/databases/{self.database_id}/query", 
            headers=headers).json()
        records = response["results"]
        return records
    
    def get_raw_value(self, item):
        item_type = item['type']
        if type(item[item_type]) is list:
            if item[item_type][0]['type'] == 'text':
                return item[item_type][0]['plain_text']
        return item[item_type]

    def NotionToPandas(self, records):
        all_values = []   
        for record in records:
            properties = record['properties']
            all_values.append({
                'Nomor': self.get_raw_value(properties['Nomor']),
                'Nama': self.get_raw_value(properties['Nama']),
                'Job': self.get_raw_value(properties['Job']),
                })
        df = pd.DataFrame(all_values)
        return df
    
    def getSheetData(self):
        return self.sheet.get_all_values()
    
    def WriteToSheet(self,df,index,len_sheet_data):
        dfList = df.values.tolist()
        if len_sheet_data==1:
            set_with_dataframe(self.sheet, df[1:], row=1, col=1, include_column_header=True, resize=False, allow_formulas=True)
        else:
            if len(index)>1:
                for item in index:
                    self.sheet.append_row(dfList[item])
            elif len(index)==1:
                self.sheet.append_row(dfList[index[0]])
    
    def delete_data(self,index):
        if len(index)>1:
            for item in index:
                self.sheet.delete_row(item+2)
        elif len(index)==1:
            self.sheet.delete_row(index[0]+2)

    def checkToAdd(self):
        notion_data = self.NotionToPandas(self.load_page())
        notion_dataList = notion_data.values.tolist()
        sheet_data = self.getSheetData()
        len_sheet_data = len(sheet_data)
        sheet_data = sheet_data[1:]
        list_difference = []
        index = []
        for item in notion_dataList:
            if item not in sheet_data:
                list_difference.append(item)
        if len(list_difference)>0:
            for item in list_difference:
                idx = notion_dataList.index(item)
                index.append(idx)
        if len(index) > 0:
            self.WriteToSheet(notion_data,index,len_sheet_data)
        else:
            return "Nothing changed"
  
    def checkToDelete(self):
        notion_data = self.NotionToPandas(self.load_page())
        notion_dataList = notion_data.values.tolist()
        sheet_data = self.getSheetData()
        sheet_data = sheet_data[1:]
        list_difference = []
        index = []
        for item in sheet_data:
            if item not in notion_dataList:
                list_difference.append(item)
        if len(list_difference) > 0:
            for item in list_difference:
                idx = sheet_data.index(item)
                index.append(idx)
        if len(index)>0:
            self.delete_data(index)