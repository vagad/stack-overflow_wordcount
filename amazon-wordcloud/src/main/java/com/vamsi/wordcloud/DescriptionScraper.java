package com.vamsi.wordcloud;
import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;


public class DescriptionScraper
{
	public Document doc;
	public DescriptionScraper(String url) throws IOException{
		doc = Jsoup.connect(url).get();

	}

	public String productDetails()
	{
        String question = doc.select("#question .post-text").text();
		
		return question;
	}
}