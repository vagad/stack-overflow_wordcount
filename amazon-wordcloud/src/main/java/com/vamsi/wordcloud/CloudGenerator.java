package com.vamsi.wordcloud;

import static spark.Spark.*;
import spark.Request;
import spark.Response;
import spark.Route;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;

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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class CloudGenerator 
{

    
    public static TransportClient client;

    public CloudGenerator() throws UnknownHostException{
   		client = new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress
        	(InetAddress.getByName("localhost"), 9300));

        boolean urlExist = client.admin().indices()
		    .prepareExists("url_listing")
		    .execute().actionGet().isExists();

		boolean wordsExist = client.admin().indices()
		    .prepareExists("word_listing")
		    .execute().actionGet().isExists();
		
		if(!urlExist){
			client.admin().indices().prepareCreate("url_listing")
		        .setSettings(Settings.builder()             
		                .put("index.number_of_shards", 3)
		                .put("index.number_of_replicas", 2)
		        )
		        .addMapping("url_page", "{\n" +                
	                "    \"url_page\": {\n" +
	                "      \"properties\": {\n" +
	                "        \"url\": {\n" +
	                "          \"type\": \"string\",\n" +
	                "          \"index\": \"not_analyzed\"\n" +
	                "        }\n" +
	                "      }\n" +
	                "    }\n" +
	                "  }")
		        .get();
		} 

		if(!wordsExist){
			client.admin().indices().prepareCreate("word_listing")
	        .setSettings(Settings.builder()             
	                .put("index.number_of_shards", 3)
	                .put("index.number_of_replicas", 2)
	        )
	        .addMapping("word_details", "{\n" +                
	                "    \"word_details\": {\n" +
	                "      \"properties\": {\n" +
	                "        \"word\": {\n" +
	                "          \"type\": \"string\",\n" +
	                "          \"index\": \"not_analyzed\"\n" +
	                "        },\n" +
	                "        \"count\": {\n" +
	                "          \"type\": \"integer\"\n" +
	                "        }\n" +
	                "      }\n" +
	                "    }\n" +
	                "  }")
	        .get(); 
		}
    }

    public static void main( String[] args ) throws UnknownHostException
    {
		CloudGenerator init = new CloudGenerator();
		
		post("/", (request, response) -> {
    		String url = request.queryParams("url");
    		SearchResponse sr = client.prepareSearch("url_listing")
    			.setTypes("url_page")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.matchQuery("url", url))                 // Querte
		        .get();

		    long hits = sr.getHits().totalHits();
		    
		    if(hits > 0)
		    {
		    	String state = String.format("%s - Already Exists!\n", url);
		    	response.status(400); // 201 Created
	        	return state;
		    }else{
		    	IndexResponse creationrs = client.prepareIndex("url_listing", "url_page")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("url", url)
			                    .endObject()
			                  )
			        .execute()
			        .actionGet();
			    String state = String.format("%s - Created!\n", url);
			    
			    try{
			    	DescriptionScraper scrapeQuestion = new DescriptionScraper(url);
			    	String wordData = scrapeQuestion.productDetails();

			    	System.out.println("Scraping complete");
			    	DiagramProducer writer= new DiagramProducer();
			    	// DiagramProducer writer= new DiagramProducer(url, wordData);
			    	System.out.println("Producer Created");
			    	writer.kafkaWrite(url, wordData);

			    }catch(IOException e){
			    	response.status(404);
			    	String failState = String.format("%s - Bad URL!\n", url);
			    	return failState;
				}catch(Exception e){
					System.out.println(e.getMessage());
					System.out.println(e.getStackTrace());
					System.out.println(e.getCause());
					e.printStackTrace();
				}
			    response.status(201); // 201 Created
	        	return state;
		    }
		});

		get("/", (request, response) -> {
    		SearchRequestBuilder requestBuilder = client.prepareSearch("word_listing")
                                            .setTypes("word_details"); 

			SearchHitIterator hitIterator = new SearchHitIterator(requestBuilder);
			StringBuilder finalVal = new StringBuilder();
	        ObjectMapper mapper = new ObjectMapper();

			while (hitIterator.hasNext()) {
			    SearchHit hit = hitIterator.next();
			    String sourceAsString = hit.getSourceAsString();
	           	Word wordInfo = mapper.readValue(sourceAsString, Word.class);
	            String hitString = String.format("%s - %d\n", wordInfo.getWord(), wordInfo.getCount());
		    	finalVal.append(hitString);
			}

	        response.status(200); // 201 Created
	        return finalVal.toString();
		});
    }
}
