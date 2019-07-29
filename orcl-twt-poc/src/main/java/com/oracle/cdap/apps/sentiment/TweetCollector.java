package com.oracle.cdap.apps.sentiment;

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TweetCollector extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TweetCollector.class);

  private OutputEmitter<Tweet> output;

  private CollectingThread collector;
  private BlockingQueue<Tweet> queue;

  private Metrics metrics;

  private TwitterStream twitterStream;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    Map<String, String> args = context.getRuntimeArguments();

    if (args.containsKey("disable.public")) {
      String publicArg = args.get("disable.public");
      LOG.info("Public Twitter source turned off (disable.public={})", publicArg);
      return;
    }

    if (!args.containsKey("oauth.consumerKey") || !args.containsKey("oauth.consumerSecret")
     || !args.containsKey("oauth.accessToken") || !args.containsKey("oauth.accessTokenSecret")) {
      final String CREDENTIALS_MISSING = "Twitter API credentials not provided in runtime arguments.";
      LOG.error(CREDENTIALS_MISSING);
//      throw new IllegalArgumentException(CREDENTIALS_MISSING);
    }

    queue = new LinkedBlockingQueue<Tweet>(10000);
    collector = new CollectingThread();
    collector.start();
  }

  @Override
  public void destroy() {
    if (collector != null) {
      collector.interrupt();
    }
    if (twitterStream != null) {
      twitterStream.cleanUp();
      twitterStream.shutdown();
    }
  }

  @Tick(unit = TimeUnit.MILLISECONDS, delay = 100)
  public void collect() throws InterruptedException {
    if (this.queue == null) {
      // Sleep and return if public timeline is disabled
      Thread.sleep(1000);
      return;
    }
    int batchSize = 2;

    for (int i = 0; i < batchSize; i++) {
      Tweet tweet = queue.poll();
      if (tweet == null) {
        break;
      }

      metrics.count("public.total", 1);
      output.emit(tweet);
      System.out.println("Sentiment :"+tweet.getSentiment()+" : "+tweet.getText());
    }
  }

  private class CollectingThread extends Thread {

    @Override
    public void run() {
      try {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setAsyncNumThreads(1);

        Map<String, String> args = getContext().getRuntimeArguments();

        // Override twitter4j.properties file, if provided in runtime args.
        //if (args.containsKey("oauth.consumerKey") && args.containsKey("oauth.consumerSecret")
        // && args.containsKey("oauth.accessToken") && args.containsKey("oauth.accessTokenSecret")) {
                cb.setOAuthConsumerKey("KreHCubYI31WLs7k5xbfARV43")
                .setOAuthConsumerSecret("8qPOtzHAyyW0KCUgzNh7DGY7tfYOPxdAu5bn9CdAzGxa4tcYWy")
                .setOAuthAccessToken("8021782-Ev4iLGNzjd6qZJcQui6ncCKO2rr8BF2E6A0IsCBoLq")
                .setOAuthAccessTokenSecret("Hq7N3xRozkV2vcuQQF2cbrtVJ2MUhmKMxc9mfr3GV8ES9");
        //}

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        //twitterStream.filter("india");

        StatusListener listener = new StatusAdapter() {
          @Override
          public void onStatus(Status status) {
            String text = status.getText();
            String lang = status.getLang();
            metrics.count("lang." + lang, 1);
            if (!lang.equals("en")) {
              metrics.count("otherLang", 1);
              return;
            }
            try {
              queue.put(new Tweet(text, status.getCreatedAt().getTime()));
            } catch (InterruptedException e) {
              LOG.warn("Interrupted writing to queue", e);
              return;
            }
          }

          @Override
          public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            LOG.error("Got track limitation notice:" + numberOfLimitedStatuses);
          }

          @Override
          public void onException(Exception ex) {
            LOG.warn("Error during reading from stream" + ex.getMessage());
          }
        };

        FilterQuery fq = new FilterQuery();
        String keywords[] = {"#csk"};

        fq.track(keywords);


        twitterStream.addListener(listener);
        twitterStream.filter(fq);
        //twitterStream.sample();

      } catch (Exception e) {
        LOG.error("Got exception {}", e);
      } finally {
        LOG.info("CollectingThread run() exiting");
      }
    }
  }

//    public static void main(String[] args) {
//        TweetCollector tc = new TweetCollector();
//        try {
//            tc.initialize();
//            tc.collect();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
