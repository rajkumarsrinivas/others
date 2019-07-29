package com.oracle.cdap.apps.sentiment;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.metrics.Metrics;
import com.google.common.base.Charsets;


public class CountSentimentFlowlet extends AbstractFlowlet {
  static final String NAME = "CountSentimentFlowlet";

  @UseDataSet(TwitterSentimentApp.TABLE_NAME)
  private Table sentiments;

  @UseDataSet(TwitterSentimentApp.TIMESERIES_TABLE_NAME)
  private TimeseriesTable textSentiments;

  Metrics metrics;

  @Batch(10)
  @ProcessInput
  public void process(Tweet tweet) {
    String sentence = tweet.getText();
    String sentiment = tweet.getSentiment();

    System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%#############################");
    System.out.println(tweet.getText());
    System.out.println(tweet.getSentiment());
    System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%#############################");

    metrics.count("sentiment." + sentiment, 1);
    sentiments.increment(new Increment("aggregate", sentiment, 1));
    textSentiments.write(new TimeseriesTable.Entry(sentiment.getBytes(Charsets.UTF_8),
                                                   sentence.getBytes(Charsets.UTF_8),
                                                   System.currentTimeMillis()));

  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Updates the sentiment counts");
  }
}