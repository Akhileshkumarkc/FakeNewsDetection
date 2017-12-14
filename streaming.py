from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from urllib.request import urlopen
from bs4 import BeautifulSoup


def filterAndGetUrl(tweet):
    if(tweet['user']["url"]==None or tweet['user']["lang"]!='en'):
        return False
    
    if("youtube" in tweet['user']["url"] or "youtu.be" in tweet['user']["url"] or "twitter" in tweet['user']["url"]):
        return False
    else:
        return True


def getUrlText(url):  
    try:
        with urlopen(url) as response:
            html = response.read()
            
#            soupHeader = BeautifulSoup(html).find(['h1','h2','h3','h4'])
#            header = ""
#            for h in soupHeader:
#                data = h.get_text()
#                if(data!=""):
#                    header += ' '+data
                    
            soup = BeautifulSoup(html,'html.parser').body
            
            divs = soup.find_all('div')
            text = ""
            for div in divs:
                data = div.stripped_strings
                for string in data:
                    if(string!=""):
                        text += ' '+string
#            return [header,text]
            return text

    except:
        return ""
    return ''

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

    
    urls = parsed.filter(filterAndGetUrl).map(lambda tweet : getUrlText(tweet['user']["url"]))

	#Print the User tweet counts
    urls.pprint()

	#Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()
