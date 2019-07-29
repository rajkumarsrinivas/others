package com.oracle.cdap.apps.sentiment;

import co.cask.cdap.api.flow.AbstractFlow;

/**
 * Flow for sentiment analysis.
 */
public class SentimentAnalysisFlow extends AbstractFlow {
  static final String FLOW_NAME = "TwitterSentimentAnalysis";

  @Override
  protected void configure() {
    setName(FLOW_NAME);
    setDescription("Analysis of text to generate sentiments");
    addFlowlet(new TweetCollector());
    addFlowlet(new PythonAnalyzer());
    addFlowlet(new CountSentimentFlowlet());
    connectStream(TwitterSentimentApp.STREAM_NAME, new TweetCollector());
    connect(new TweetCollector(), new PythonAnalyzer());
    connect(new PythonAnalyzer(), new CountSentimentFlowlet());
  }
}
