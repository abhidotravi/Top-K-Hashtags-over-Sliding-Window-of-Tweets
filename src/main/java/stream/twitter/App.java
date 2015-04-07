package stream.twitter;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.*;

/**
 * This is a simple implementation of a top-k over sliding window of Tweets.
 * The purpose is to show how the feature "Trending Topics" works.
 * @author abhishekravi
 *
 **/
public class App 
{
	static final int queueSize = 50000;
	static final int k = 10;
	
	//Stores all latest N hashTags received - The Sliding Window
	static LinkedBlockingQueue<String> myQueue = new LinkedBlockingQueue<String>(queueSize);
	
	//Tracks the count of the hashTags currently in the Queue
	static HashMap<String, Integer> myHash = new HashMap<String, Integer>();
	
	//Heap used to keep track of top 'K' items based on their count
	static PriorityQueue<String> topk = new PriorityQueue<String>(k, new TopKComparator(myHash));
	
	// Following are created as member variables to reduce overhead of
	// instantiating and re-instantiating them every time in the callback
	static String tweetText = new String();
	static String hashTag = new String();
	static String removedHashTag = new String();
	static int hashTagCount;

	
    public static void main( String[] args ) throws Exception
    {

        StatusListener listener = new StatusListener() {
        	
        	//A callback function that is called whenever we receive a Tweet
            public void onStatus(Status status) {
            	
            	//Extract the text from the Tweet
            	tweetText = status.getText();
                
                //Extract hashtags
                Pattern pattern = Pattern.compile("#\\w+");
                Matcher myMatch = pattern.matcher(tweetText);
                
                //Loop through the substring to find a match
                while(myMatch.find()) {
                	
                	//When there is a match, it is the hashTag
                	hashTag = myMatch.group();

                	//Increase the count of the hashTag if already exists
                	//Else push it to Hash Map with count 1
                	if(myHash.containsKey(hashTag)) {
                		myHash.put(hashTag, myHash.get(hashTag) + 1);
                	} else
                		myHash.put(hashTag, 1);
                	
                	//If the Queue storing the hashTag is full, remove from front
                	if(myQueue.size() == queueSize) {
                		removedHashTag = myQueue.poll();
                		hashTagCount = myHash.get(removedHashTag);
                		if(hashTagCount > 1)
                			myHash.put(removedHashTag, hashTagCount - 1);
                		else
                			myHash.remove(removedHashTag);
                	}
                	
                	//Enqueue the hashTag to the Queue
                	myQueue.offer(hashTag);
                	
                	//Logic for top-k based on Heap
                	if(!topk.contains(hashTag)) {
                		//if heap does not contain the hashTag, add it
                		topk.add(hashTag);
                	} else {
                		//if it contains, remove and add it again
                		//so that it can re-adjust
                		topk.remove(hashTag);
                		topk.add(hashTag);
                	}
                	
                	//If heap is full, remove the element with least count
                	while(topk.size() > k) topk.poll();
                	
                	//System.out.println(topk.toString());
                	//System.out.println(myHash.toString());
                }
                
            }


            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //Not needed here
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            	//Not needed here
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            public void onException(Exception ex) {
            	System.out.println("This is just an Exception from Twitter4j, Nothing to Panic!!");
                ex.printStackTrace();
            }
        };
        
        //Get an instance of scanner
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("Hit ENTER to begin!!!");
        
        //Flush already existing input
        scanner.nextLine();
        
        /*
         * Obtain consumerKey, consumerSecret,  accessToken and accessTokenSecret
         * from the user.
         */
        System.out.println("Enter your CONSUMER KEY");
        String consumerKey = scanner.nextLine();
        System.out.println("Enter your CONSUMER SECRET KEY");
        String consumerSecret = scanner.nextLine();
        System.out.println("Enter your ACCESS TOKEN");
        String accessToken = scanner.nextLine();
        System.out.println("Enter your ACCESS TOKEN SECRET");
        String accessTokenSecret = scanner.nextLine();   
       
        //Create a Twitter Stream object
		TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		//Add a listener
		twitterStream.addListener(listener);
		
		//Set Authentication parameters
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		
		
		System.out.println("How many KEYWORDS you want to subscribe? Enter a number: ");
		
		int numKeywords = Integer.parseInt(scanner.nextLine());
		
		if(numKeywords == 0) {
			//If there are no keywords the user wishes to subscribe
			twitterStream.sample();
		} else {
			String[] keywords = new String[numKeywords];
			
			//Get the keywords user wishes to subscribe
			for(int i = 0; i < numKeywords; i++) {
				System.out.println("Keyword " + (i+1) + " is? ");
				keywords[i] = scanner.nextLine();
			}
			
			FilterQuery query = new FilterQuery().track(keywords);
			twitterStream.filter(query);
		}
        
		//Close the scanner
        scanner.close();
        
        //Query top-k periodically
        while(true){
        	try {
        	    Thread.sleep(5000); //1000 milliseconds is one second.
        	} catch(InterruptedException ex) {
        	    Thread.currentThread().interrupt();
        	}
        	
        	//Equivalent to querying for top-k
        	System.out.println(topk.toString());

        }
    }
}

/**
 * Comparator used by Priority Queue.
 * Compare the items based on their count.
 **/
class TopKComparator implements Comparator<String> {
	HashMap<String, Integer> h;

	public TopKComparator(HashMap<String, Integer> h) {
		this.h = h;
	}
	
	public int compare(String s1, String s2) {
		return (int)(h.get(s1) - h.get(s2));
	}
}