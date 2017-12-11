# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 17:54:19 2017

@author: shalin
"""

from csv import DictReader

class DataSet:
    
    def __init__(self):
        
        print("Reading Dataset........")
        train_body_path = 'C:/Users/shalin/Desktop/Fake_News_Detection/Stance_Detection/BaseLine/fnc-baseline-git-repo/fnc/train_bodies.csv'
        train_stances_path = 'C:/Users/shalin/Desktop/Fake_News_Detection/Stance_Detection/BaseLine/fnc-baseline-git-repo/fnc/train_stances.csv'  
        
        train_body = self.read(train_body_path)
        self.train_stances = self.read(train_stances_path)
        self.articles = {}
        
        for s in train_body:
            s['Body ID'] = int(s['Body ID'])
            
        for a in train_body:
            self.articles[int(a['Body ID'])] = a['articleBody']
            
        print("Total Bodies...."+str(len(self.articles)))
        print("Total Stances...."+str(len(self.train_stances)))
        print("Reading Complete")
        
    def read(self,path):
        
        lines = []
        with open(path,'r',encoding='utf-8') as readcsv:
            r = DictReader(readcsv)
            
            for line in r:
                lines.append(line)
        return lines  


      
