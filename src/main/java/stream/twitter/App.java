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
 * Top-k over a Sliding Window
 * @author abhishekravi
 *
 */
public class App 
{
	static final int queueSize = 10000;
	static final int k = 10;
	static LinkedBlockingQueue<String> myQueue = new LinkedBlockingQueue<String>(queueSize);
	static HashMap<String, Integer> myHash = new HashMap<String, Integer>();
	static PriorityQueue<String> topk = new PriorityQueue<String>(k, new TopKComparator(myHash));
	
	// Following are created as member variables to reduce overhead of
	// instantiating and re-instantiating them every time in the callback
	static String tweetText = new String();
	static String screenName = new String();
	static String hashTag = new String();
	static String removedHashTag = new String();
	static int hashTagCount;

	
    public static void main( String[] args ) throws Exception
    {

        StatusListener listener = new StatusListener() {
        	
            public void onStatus(Status status) {
            	
            	tweetText = status.getText();
            	screenName = status.getUser().getScreenName();
                
                //Extract hashtags
                Pattern pattern = Pattern.compile("#\\w+");
                Matcher myMatch = pattern.matcher(tweetText);
                
                while(myMatch.find()) {
                	hashTag = myMatch.group();

                	if(myHash.containsKey(hashTag)) {
                		myHash.put(hashTag, myHash.get(hashTag) + 1);
                	} else
                		myHash.put(hashTag, 1);
                	
                	if(myQueue.size() == queueSize) {
                		removedHashTag = myQueue.poll();
                		hashTagCount = myHash.get(removedHashTag);
                		if(hashTagCount > 1)
                			myHash.put(removedHashTag, hashTagCount - 1);
                		else
                			myHash.remove(removedHashTag);
                	}                		
                	myQueue.offer(hashTag);
                	
                	if(!topk.contains(hashTag)) {
                		topk.add(hashTag);
                	} else {
                		topk.remove(hashTag);
                		topk.add(hashTag);
                	}
                	
                	while(topk.size() > k) topk.poll();
                	
                	System.out.println(topk.toString());
                	//System.out.println(myHash.toString());
                }
                
                
                
            }


            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
               
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        
        //Get an instance of scanner
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("Hit on Enter to begin!");
        
        //Flush already existing input
        scanner.nextLine();
        
        /*
         * Obtain consumerKey, consumerSecret,  accessToken and accessTokenSecret
         * from the user.
         */
        System.out.println("Enter your Consumer Key");
        String consumerKey = scanner.nextLine();
        System.out.println("Enter your Consumer Secret Key");
        String consumerSecret = scanner.nextLine();
        System.out.println("Enter your Access Token");
        String accessToken = scanner.nextLine();
        System.out.println("Enter your Access Token Key");
        String accessTokenSecret = scanner.nextLine();   
       
        
		TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		
		
		
		System.out.println("Enter the number of keywords you want to subscribe for: ");
		int numKeywords = Integer.parseInt(scanner.nextLine());
		
		if(numKeywords == 0) {
			twitterStream.sample();
		} else {
			String[] keywords = new String[numKeywords];
			
			for(int i = 0; i < numKeywords; i++) {
				System.out.println("Enter keyword number " + (i+1));
				keywords[i] = scanner.nextLine();
			}
			
			FilterQuery query = new FilterQuery().track(keywords);
			twitterStream.filter(query);
		}
        
        scanner.close();
    }
}

class TopKComparator implements Comparator<String> {
	HashMap<String, Integer> h;

	public TopKComparator(HashMap<String, Integer> h) {
		this.h = h;
	}
	
	public int compare(String s1, String s2) {
		return (int)(h.get(s1) - h.get(s2));
	}
}