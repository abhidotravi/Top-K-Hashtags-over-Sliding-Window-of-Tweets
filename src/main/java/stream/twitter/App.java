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

import java.util.Scanner;

/**
 * Top-k over a Sliding Window
 * @author abhishekravi
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        StatusListener listener = new StatusListener() {
        	
            public void onStatus(Status status) {
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                
            }


            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
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
