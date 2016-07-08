from __future__ import print_function
import json
import semantria
import uuid
import time
import csv
import MySQLdb
import re
from geopy.geocoders import Nominatim
import matplotlib.pyplot as plt


db = MySQLdb.connect(host="localhost", user="root", passwd="avk287",
db="avk287")
cursor = db.cursor()


serializer = semantria.JsonSerializer()
session = semantria.Session("e1eef79b-f58f-4e79-830c-9203ae09a473", "4a2891a0-2df0-4672-b3e9-5392f2b07256", serializer, use_compression=True)
geolocator = Nominatim()


def plot1(result_q1):

    xaxis = []
    for x in range (0 , len(result_q1)):
        xaxis.append(x)
    yaxis = []
    label= []
    for item in result_q1:
        label.append(item[0])    
        yaxis.append(item[1])

    plt.bar(xaxis, yaxis, align='center')
    plt.xticks(xaxis, label)
    plt.show()


def plot2(result_q2):

    xaxis = []
    print (result_q2)
    print (len(result_q2))
    for x in range (0 , len(result_q2)):
        xaxis.append(x)
    yaxis = []
    label= []
    
    for item in result_q2:
        print (item[0])
        print (item[1])
        
        label.append(item[2])    
        yaxis.append((item[0]) )
         
    plt.bar(xaxis, yaxis, align='center')
    plt.xticks(xaxis, label)

    plt.show()


def plot3(result_q3):

    xaxis = []
    print (result_q3)
    print (len(result_q3))
    for x in range (0 , len(result_q3)):
        xaxis.append(x)
    yaxis = []
    label= []
    for item in result_q3:
        print (item[0])
        print (item[1])
        
        label.append(item[1] )   
        yaxis.append(item[0])

    plt.bar(xaxis, yaxis, align='center')
    plt.xticks(xaxis, label)
    plt.show()


def semant(tweets_data):

    count =0 
    for text in tweets_data:   
        try:
            print (text['text'])
            doc = {"id": str(uuid.uuid4()).replace("-", ""), "text": text['text']}
            status = session.queueDocument(doc)
            text['sid'] = doc["id"]
            print (status)
            print (text['sid'])
            if status == 202:
                print("\"", doc["id"], "\" document queued successfully.", "\r\n")
                count = count + 1
                print (count)
        except:
            pass




    results = []
    sentiment = "Neutral"
    length = len(tweets_data)
    print (length)
    while len(results) < length:
        
        print("Retrieving your processed results...", "\r\n")
        time.sleep(2)
        # get processed documents
        status = session.getProcessedDocuments() 

        results.extend(status)
        print (len(results))


    print ("check")
    for item in tweets_data:   
              
        for data in results:  
            

            if(data['id'] == item['sid']):
               
                '''key = "location"
                key1 = "latitude"
                key2 = "longitude"
                item.setdefault(key,[])
                item.setdefault(key1,[])
                item.setdefault(key2,[])'''

                item['senti_score'] = data['sentiment_score']
                                    
                try:
                    for entity in data['entities']: 
                        
                        if(entity['entity_type'] == "Place"):
                            print ("wtf")
                            print (entity['title'])
                            '''item['location'].append(entity['title'])
                            item['latitude'].append(geolocator.geocode(entity['title']).latitude)
                            item['longitude'].append(geolocator.geocode(entity['title']).longitude)'''
                            item['location'] = (entity['title'])
                            item['latitude'] = (geolocator.geocode(entity['title']).latitude)
                            item['longitude'] = (geolocator.geocode(entity['title']).longitude)
                            print(item["location"])
                            print (item['latitude'])
                            print (item['longitude'])
                    
                         
                        if(item['senti_score'] < 0):
                         item['senti_type'] = 'Negative'
                        elif(item['senti_score'] > 0):
                         item['senti_type'] = 'positive'
                        else:
                         item['senti_type'] = 'Neutral'
                except:
                    pass
        #print (item)
    return tweets_data




tweets_data = []
tweets_file = open('data1.txt', "r")

for line in tweets_file:
        try:
         tweet = json.loads(line)
         tweets_data.append(tweet)
         
        except:
         continue

tdata = semant(tweets_data)

f = csv.writer(open("twitter.csv","w"))
f.writerow(["U_ID","Tweet","Timestamp","Source","Language","Sentiment","Sentiment_score","Location","Latiutde","Longitude"])

'''for i in tdata:
        try:
            f.writerow([i["id"],i["text"].encode('utf-8'),i["timestamp_ms"],i["source"].encode('utf-8'),i["lang"].encode('utf-8'),i["senti_type"],i["senti_score"],i["location"],str(i["latitude"]),str(i["longitude"])])
        except:
            pass'''


for i in tdata:
    try:
        cursor.execute('''INSERT into o_twitter (Id , t_text , time , source , language , sentiment , s_score , location , lat , lon) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', ([i["id"] ,i["text"].encode('utf-8') ,i["timestamp_ms"],i["source"].encode('utf-8'),i["lang"].encode('utf-8'),i["senti_type"],i["senti_score"],i["location"].encode('utf-8'),str(i["latitude"]),str(i["longitude"])  ]))
        cursor.execute('''INSERT into twitter (Id , t_text , time , source , language , location) values (%s,%s,%s,%s,%s,%s)''', ([i["id"] ,i["text"].encode('utf-8') ,i["timestamp_ms"],i["source"].encode('utf-8'),i["lang"].encode('utf-8'),i["location"].encode('utf-8') ]))
        cursor.execute('''INSERT into sentiment (Id , sentiment , s_score) values (%s,%s,%s)''', ([i["id"] , i["senti_type"],i["senti_score"]]))
        #cursor.execute('''INSERT into location (location , lat , lon )  values (%s,%s,%s)''', (i["location"],str(i["latitude"]),str(i["longitude"]) ]))
        db.commit()
    except:
        pass

cursor.execute('''INSERT ignore into  location (location , lat , lon ) select o.location , o.lat , o.lon  from o_twitter o group by location ''')
q1 = """ select language , count(*) from twitter group by language;  """
q2 = """ select Avg(s.s_score)  ,  count(t.Id)  , t.location  from twitter t , sentiment s   where t.Id = s.Id group by t.location;      """
q3 = """ select count(t.Id) , t.location from twitter t group by t.location;"""

cursor.execute(q1)
result_q1 = cursor.fetchall()
cursor.execute(q2)
result_q2 = cursor.fetchall()
cursor.execute(q3)
result_q3 = cursor.fetchall()




 
db.commit()
db.close()

plot1(result_q1)    
plot2(result_q2)
plot3(result_q3)





