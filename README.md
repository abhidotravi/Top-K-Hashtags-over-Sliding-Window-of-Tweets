# Top-K Hashtags over a Sliding Window of Tweets
Application to display top-k hashtags over a sliding window of tweets. 
User is given the option to subscribe for particular keywords.

1. The sliding window contains the latest tweets seen over last few units of time or contains the last N tweets. In this application I have made the assumption that the window refers to the window of latest N tweets. Hence whenever N+1 tweet comes, the first tweet is removed.

2. The application is built with the assumption that the user has the required credentials to subscribe for tweets. These include:
CONSUMER KEY, CONSUMER SECRET, ACCESS TOKEN, ACCESS TOKEN SECRET. 

3. An assumption that sometimes the user may want to subscribe for tweets with particular keywords and see the top-k hashtags for those topics, is made. However, if user has no preference it is still possible to indicate that and subscribe for tweets.

4. This is a simple application to show how stream processing is done.

5. The application is developed as a maven project and can be run on Eclipse or command-line.

6. The application is designed to run forever until the user manually ends it.


STEPS TO INSTALL AND RUN
---------------------------
1. Ensure you have CONSUMER KEY, CONSUMER SECRET, ACCESS TOKEN, ACCESS TOKEN SECRET handy as you will need to give these as input.

2. Extract topk.tar

3. cd topk/

4. RUN the following commands

mvn clean compile assembly:single
java -cp target/topk-0.0.1-SNAPSHOT-jar-with-dependencies.jar stream.twitter.App

(NOTE: IF you want to run on Eclipse please find the instructions at the end of the document.)

User Input
-------------

1. Click on Enter to begin

2. Enter CONSUMER KEY, CONSUMER SECRET, ACCESS TOKEN, ACCESS TOKEN SECRET in the same order.

3. Enter number of Keywords to subscribe for. YOU CAN ENTER 0 IF YOU DO NOT HAVE A PREFERENCE.

4. If you have asked to enter M Keywords, enter M keywords one after the other.

5. Hit on Stop to stop the application.

Refer to sample1, sample2 and sample3 snapshots in “results” folder to see sample user inputs and outputs.


Implementation details
-------------------------

1. User is asked to enter the credentials required to establish a twitter stream connection.

2. User is asked to enter number of keywords to subscribe for. ZERO is entered if there is no preference. If preference is given, only those tweets with the entered keywords is subscribed.

3. OnStatus function is called every time a tweet is obtained.

4. Text is extracted from Tweet and Hashtag is extracted from text using Regex.

5. HashTag is stored in a queue that is of size 50000. The size can be increased or decreased. 

6. Count of each Hashtag stored in queue is maintained in a hash map.

7. Decrease count of a hashtag when removed from queue and similarly increase count when added to the queue.

8. Maintain a heap that contains top-k hashtags i.e. k hashtags with highest counts. A comparator is used to compare elements in heap. This comparator compares elements based on their counts in the map.

9. Every 5 seconds, the heap is printed to see the top k elements. In our case k = 10.


Eclipse Installations
--------------

1. Eclipse version with maven plugin

http://eclipse.org/m2e/

This version of Eclipse comes with maven plugin integrated.

You can add maven plugin to your existing eclipse refer to the following link:

http://stackoverflow.com/questions/8620127/maven-in-eclipse-step-by-step-installation

Eclipse Steps for Compilation
-----------------------
1. Extract the topk.tar

2. Once maven plugin is enabled on Eclipse, go to File -> Import -> Existing Maven Projects and give the path of pom.xml on your machine. Click on “Finish” and the maven project is loaded on eclipse.

3. Now hit “RUN” and all the dependencies will automatically be downloaded and the code will be compiled and the application will run.
