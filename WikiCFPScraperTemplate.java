import java.net.*;
import java.io.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


public class WikiCFPScraperTemplate {
	public static int DELAY = 7;
	public static void main(String[] args) {
	
		try {
			
			
			String category = "data mining";
			//String category = "databases";
			//String category = "machine learning";
			//String category = "artificial intelligence";
			
	        int numOfPages = 20;
	        
	        //create the output file
	        File file = new File("wikicfp_crawl.txt");
	        file.createNewFile();
	        FileWriter writer = new FileWriter(file); 
	       
	    
	        
	        //now start crawling the all 'numOfPages' pages
	        for(int i = 1;i<=numOfPages;i++) {
	        	//Create the initial request to read the first page 
				//and get the number of total results
	        	String linkToScrape = "http://www.wikicfp.com/cfp/call?conference="+
	        				      URLEncoder.encode(category, "UTF-8") +"&page=" + i;
	        	String content = getPageFromUrl(linkToScrape);	        	
	        	//parse or store the content of page 'i' here in 'content'
	        	//YOUR CODE GOES HERE
	        	writer.write(content);
	        	System.out.println(content);
	        		        	
	        	//IMPORTANT! Do not change the following:
	        	Thread.sleep(DELAY*1000); //rate-limit the queries
	        }

        writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * Given a string URL returns a string with the page contents
	 * Adapted from example in 
	 * http://docs.oracle.com/javase/tutorial/networking/urls/readingWriting.html
	 * @param link
	 * @return
	 * @throws IOException
	 */
	public static String getPageFromUrl(String link) throws IOException {
		URL thePage = new URL(link);
        URLConnection yc = thePage.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                                    yc.getInputStream()));
        String inputLine;
        String output = "";
        while ((inputLine = in.readLine()) != null) {
        	output += inputLine + "\n";
        }
        in.close();
		return output;
	}
	
	
	
	}


