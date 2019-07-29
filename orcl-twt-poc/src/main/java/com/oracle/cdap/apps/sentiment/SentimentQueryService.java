package com.oracle.cdap.apps.sentiment;

import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;

/**
 * A {@link Service} that retrieves the aggregates timeseries sentiment data.
 */
public class SentimentQueryService extends AbstractService {
  static final String SERVICE_NAME = "SentimentQuery";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("Queries data relating to tweets' sentiments");
    addHandler(new SentimentQueryServiceHandler());
  }
}
