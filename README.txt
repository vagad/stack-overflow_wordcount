Overview: 
This tool has the ability to create a listing of all words and their frequency across the questions asked on StackOverflow. 
To maintain scalability and high efficiency, the following tools were used: Elasticsearch, Spark (java REST framework), 
Spark Streaming, Kafka, Bloom Filters, and JSoup. ElasticSearch was used as the primary search engine/data store
to provide fast, scalable searching for urls and words. The Spark REST framework was used for a easy, lightweight REST API 
for the overall program. Kafka was used as a message broker to convey the messages since it can protect from bursts, has persistence,
and is extremely scalable/efficient. Bloom Filters were used to filter out unnecessary word (like “the”, “but”, etc), since they provide an 
extremely efficient method to filter out words. They reach this space/time efficiency through allowing false positive at a low probability 
(we take this risk under the assumption that the low probability ensure that there will only be few unnecessary words slipping through). 
Finally, JSoup was used as a simple, light-weight scraper to gather the text from StackOverflow. 

Instructions:

1. Start ElasticSearch with normal configs
2. run mvn clean install in amazon-wordcloud directory
3. run mvn exec:java -Dexec.mainClass=“com.vamsi.wordcloud.CloudGenerator” to run program
4. run mvn clean install in word-counter
5. run mvn exec:java -Dexec.mainClass=“com.vamsi.counter.WordCounter” to run program
6. Use Postico/Curl Request/Browser to get desired results 

Details on Usage:

POST /?url=<url>

Will return 201 status if there is a new URL and will return 400 status if the URL has been seen before

GET /

Will return complete listing of words alongside their counts

For using simulateRequests.sh: run bash simulateRequests.sh localhost 4567 url

Sample Curl Call:
curl -X POST "localhost:4567?url=http%3A%2F%2Fstackoverflow.com%2Fquestions%2F2835505"