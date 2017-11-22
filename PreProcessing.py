# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 19:31:12 2017

@author: shalin
"""

from DataSet import DataSet
import nltk
from tqdm import tqdm

d = DataSet()
#d.articles -> Dictionary (key = body id, value = article body)
#d.train_stances -> List (Each element is Dict) (key = 'stances,headline,body id')

lemma = nltk.WordNetLemmatizer()

class PreProcessing:
    
    def __init__(self):
        self.datadict = {}
        print("Preprocessing data....")
        self.run()

    def word_lemma(self,word):
        return lemma.lemmatize(word)

    def tokenize_lemma(self,s):
        tokens=[]
        sents = nltk.sent_tokenize(s)
        for sent in sents:
            a = []
            for word in nltk.word_tokenize(sent):
                a.append(self.word_lemma(word))
            ca = self.clean_list(a)
            tokens.append(ca)
        return tokens

    def clean_list(self,a):
        stopwords = ['*','...','.','-','"','"',',',',,']
        for sw in stopwords:
            if sw in a:
                a.remove(sw)
        return a        
        
    def clean(self,s):
        return s.lower()

    def stop_words(self,corpus):
        corpus_ws = []       
        for s in tqdm(corpus):
            sent = []
            for word in s:
                if word not in nltk.corpus.stopwords.words():
                    sent.append(word)
            corpus_ws.append(sent)
        return corpus_ws

    def generatedict(self,finalcorpus):
        self.datadict['corpus'] = finalcorpus

    
    def run(self):
    
        articles = {}
        articles = d.articles
        #stances = d.train_stances
        articles_body=''
        for article in tqdm(articles.keys()):
            articles_body += articles[article]
            
        clean_article_body = self.clean(articles_body)
        lemma_tokens = self.tokenize_lemma(clean_article_body)
        finalcorpus = self.stop_words(lemma_tokens)
        
        with open('combined_articles'+'.txt','w+'
                  ,encoding='utf-8') as w:
            w.write(str(finalcorpus))
            
        self.generatedict(finalcorpus)        

    
if __name__=="__main__":
    p = PreProcessing()
    
    
    
    
