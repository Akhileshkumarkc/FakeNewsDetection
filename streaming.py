from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import RealTimeFeatureExtraction


def filterAndGetUrl(tweet):
    if(tweet['entities']["urls"]==None or len(tweet['entities']["urls"])==0 or tweet['user']["lang"]!='en'):
        return False
    
    return True


def getUrlText(urls):  
    finaltext = ""
    finalHeader = ""
    
    for url in urls:
        link = url["expanded_url"]
        
        if(link==None or "youtube" in link or "youtu.be" in link or "twitter" in link):
            continue
        try:
            text = ""
            title = ""
            with urlopen(link) as response:
                html = response.read()
                       
                soup = BeautifulSoup(html,'html.parser').body
                
                header = soup.find_all('h1')
                
                for h1 in header:
                    title+=h1.text+" "
                
                if(h1==""):
                    header = soup.find_all('h2')
                
                    for h2 in header:
                        title+=h2.text     
                        
                paragraphs = soup.find_all(['p','div'])
                
                for p in paragraphs:                
                    text+=p.text+" "                   
                        
                finaltext += re.sub('[^0-9a-zA-Z]+', ' ', text) 
                finalHeader += re.sub('[^0-9a-zA-Z]+', ' ', title)
        except:
            pass
    
    return [finalHeader,finaltext]

def formText(text):    
    retText = re.sub(r'RT.+:\s+', '', text) 
    retText = re.sub(r'https?\S+', '', retText)
    retText = re.sub('[^0-9a-zA-Z]+', ' ', retText)
    
    return retText

def filterIfURL(tweetList):
    if (tweetList[1][0] == "" or tweetList[1][1] == ""):
        return False
    return True

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="PythonStreamingKafkaTweetCount")

	#Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

	#Create Kafka Stream to Consume Data Comes From Twitter Topic
	#localhost:2181 = Default Zookeeper Consumer Address
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
    
    #Parse Twitter Data as json
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    
    urls = parsed.filter(filterAndGetUrl) \
    .map(lambda tweet : [formText(tweet['text']),getUrlText(tweet['entities']["urls"])]) \
    .filter(filterIfURL).map( lambda x : RealTimeFeatureExtraction.extractFeatures(title=x[1][0], body=x[1][1]))

	#Print the User tweet counts
    urls.pprint()

	#Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()
