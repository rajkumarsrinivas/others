package com.oracle.cdap.apps.sentiment;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Application that analyzes sentiment of sentences as positive, negative or neutral.
 */
public class TwitterSentimentApp extends AbstractApplication {
  static final String NAME = "TwitterSentiment";
  static final String STREAM_NAME = "TweetStream";
  static final String TABLE_NAME = "TotalCounts";
  static final String TIMESERIES_TABLE_NAME = "TimeSeriesTable";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Twitter Sentiment Analysis");
    addStream(new Stream(STREAM_NAME));
    createDataset(TABLE_NAME, Table.class);
    createDataset(TIMESERIES_TABLE_NAME, TimeseriesTable.class);
    addFlow(new SentimentAnalysisFlow());
    addService(new SentimentQueryService());
  }
}
