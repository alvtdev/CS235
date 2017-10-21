import java.net.*;
import java.io.*;
import java.util.*;
import org.jsoup.Jsoup;
import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

public class WikiCFPScraperTemplate {
	public static int DELAY = 7;
	public static void main(String[] args) {
	
		try {
			
			
			//String category = "data mining";
			//String category = "databases";
			//String category = "machine learning";
			String category = "artificial intelligence";
			
	        int numOfPages = 20;
	        
	        //create the output file
	        File file = new File("wikicfp_crawl.txt");
	        file.createNewFile();
	        FileWriter writer = new FileWriter(file); 
	       

	        //first print column titles
	        //having this statement within the for loop would cause it to be printed once
	        //per page, which is not desired behavior
	        writer.write("conference_acronym\tconference_name\tconference_location\n");
	    
	        
	        //now start crawling the all 'numOfPages' pages
	        for(int i = 1;i<=numOfPages;i++) {
	            
	        	//Create the initial request to read the first page 
				//and get the number of total results
	        	String linkToScrape = "http://www.wikicfp.com/cfp/call?conference="+
	        				      URLEncoder.encode(category, "UTF-8") +"&page=" + i;
	        	String content = getPageFromUrl(linkToScrape);	        	
	        	//parse or store the content of page 'i' here in 'content'
	        	

	        	//Vector to store all parsed strings
	        	//The vector will store acronym, name, dates, and locations
	        	Vector data = new Vector();

	        	//parse through the html code for all useable text
	        	//narrow down to specifically just the text in the table
	        	Document doc = Jsoup.parse(content);
	        	Elements table = doc.getElementsByTag("table");
	        	Elements rows = table.select("tr");
	        	//System.out.println("Rows size: " + rows.size()); 
	        	for (int j = 1; j < rows.size(); j++) {
                    Element row = rows.get(j);
	        	    Elements tds = row.select("td");
	        	    if (tds.size() > 6) {
	        	        Elements tds2 = tds.first().children().select("tr>td");
	        	        //System.out.println(tds.first().children().select("tr>td"));
	        	        for (int k =5; k < tds2.size(); k++) {
	        	        	//now that we have specific text from the table
	        	            //append text to vector, but ignore "Expired CFPs" text
                            if (!(tds2.get(k).text().equals("Expired CFPs"))) {
                                data.addElement(tds2.get(k).text());
                                //System.out.println(tds2.get(k).text());
                            }
	        	        }
	        	    } 
	        	}

	        	//iterate through completed vector, only writing relevant information to file
	        	//use counter to keep track of number of items printed
	        	//if counter == 3, we've already printed acronym, name, and location
	        	//thus print newline instead of tab
                int itemCount = 0;
	        	for (int k = 0; k < data.size(); k++){ 
	        		//avoid printing every 2nd, 3rd, and 5th element in the vector
	        	    if ( k % 6 != 2 && k % 6 != 3 && k % 6 != 5) {
                        //System.out.print(data.elementAt(k));
                        writer.write(data.elementAt(k).toString());
                        itemCount++;
                        if (itemCount == 3) {
                           // System.out.print("\n");
                            writer.write("\n");
                            itemCount = 0;
                        }
                        else {
                            writer.write("\t");
                            //System.out.print("\t");
                        }
                    } 
                }
                //output statement to tell what page the crawler is currently on.
                //System.out.println("Page: " + i);

	        	//YOUR CODE GOES HERE
	        	//writer.write(content);
	        	//System.out.println(content);
	        		        	
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


