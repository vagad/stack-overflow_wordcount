package com.vamsi.counter;

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import java.util.*;

import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.common.settings.Settings;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.action.update.UpdateRequest;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

public class WordCounter
{
    KafkaConsumer<String, String> consumer;
    public static TransportClient client;
    BloomFilter<String> bf;

    public WordCounter() throws UnknownHostException{
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      
      consumer = new KafkaConsumer<>(props);

      client = new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress
        	(InetAddress.getByName("localhost"), 9300));

      bf = new FilterBuilder(1000, 0.01).buildBloomFilter();
      String[] stopWords = {"a", "all", "am", "an", "and", "any", "are", "arent",
    "as", "at", "be", "because", "been", "to", "from", "by",
    "can", "can't", "do", "don't", "didnt", "did", "the", "is", "or", "it", "in", "for", "he", 
    "she", "its", "won't", "but", "would", "should", "could", "why", "who", "what", "where", 
	"when"};
	
    	for(String word: stopWords){
    		bf.add(word);
    	}
    }

    public Map<String, Integer> wordCount(String message) 
    {
	  String[] strings = message.replaceAll("\\p{P}", "").split("\\s+");
	  Map<String, Integer> map = new HashMap<String, Integer> ();
	  for (String sample:strings) { 
	  	String s = sample.toLowerCase();
	  	if(! bf.contains(s)){
	  		if (!map.containsKey(s)) {  // first time we've seen this string
		 	 map.put(s, 1);
			}
			else {
			  int count = map.get(s);
			  map.put(s, count + 1);
			}
	  	}
	  }
	  return map;
	}

	public void enableCounting() throws IOException, InterruptedException, ExecutionException
	{
		try{
			consumer.subscribe(Arrays.asList("productDescriptions"));
		ObjectMapper mapper = new ObjectMapper();
      
		//print the topic name
		System.out.println("Subscribed to topic " + "productDescriptions");

		while (true) {
		 ConsumerRecords<String, String> records = consumer.poll(100);
		 for (ConsumerRecord<String, String> record : records)
		 {
			 // System.out.printf("offset = %d, key = %s, value = %s\n", 
			 //    record.offset(), record.key(), record.value());

			Map<String,Integer> frequencyData = wordCount(record.value());
			for (Map.Entry<String, Integer> entry : frequencyData.entrySet())
			{
				SearchResponse sr = client.prepareSearch("word_listing")
	    			.setTypes("word_details")
			        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			        .setQuery(QueryBuilders.matchQuery("word", entry.getKey()))               
			        .get();
		        
		        long hits = sr.getHits().totalHits();
		    
			    if(hits > 0)
			    {
			    	String id = sr.getHits().getHits()[0].id();
			    	String sourceAsString = sr.getHits().getHits()[0].getSourceAsString();
	           		Word wordInfo = mapper.readValue(sourceAsString, Word.class);
	           		int currVal = wordInfo.getCount();

	           		UpdateRequest updateRequest = new UpdateRequest();
						updateRequest.index("word_listing");
						updateRequest.type("word_details");
						updateRequest.id(id);
						updateRequest.doc(jsonBuilder()
						        .startObject()
						            .field("count", currVal + entry.getValue())
						        .endObject());
						client.update(updateRequest).get(); 	
			    }else{
			    	IndexResponse creationrs = client.prepareIndex("word_listing", "word_details")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("word", entry.getKey())
			                        .field("count", entry.getValue())
			                    .endObject()
			                  )
			        .execute()
			        .actionGet();
			    }

			}


		 }
		}
		}catch(Exception e){
			System.out.println(e.getMessage());
			System.out.println(e.getStackTrace());
			System.out.println(e.getCause());
			e.printStackTrace();
		}

		
	}

    public static void main( String[] args ) throws UnknownHostException, IOException, InterruptedException, ExecutionException
    {
        WordCounter counter = new WordCounter();
        counter.enableCounting();
        
    }
}
