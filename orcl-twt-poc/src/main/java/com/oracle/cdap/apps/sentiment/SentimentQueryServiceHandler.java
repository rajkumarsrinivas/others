package com.oracle.cdap.apps.sentiment;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.net.HttpURLConnection;
import java.util.Map;

/**
 * Handler that exposes HTTP endpoints to retrieve the aggregates timeseries sentiment data.
 */
public class SentimentQueryServiceHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new Gson();

  @UseDataSet(TwitterSentimentApp.TABLE_NAME)
  private Table sentiments;

  @UseDataSet(TwitterSentimentApp.TIMESERIES_TABLE_NAME)
  private TimeseriesTable textSentiments;

  @Path("/aggregates")
  @GET
  public void sentimentAggregates(HttpServiceRequest request, HttpServiceResponder responder) {
    Row row = sentiments.get(new Get("aggregate"));
    Map<byte[], byte[]> result = row.getColumns();
    if (result == null) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, "No sentiments processed.");
      return;
    }
    Map<String, Long> resp = Maps.newHashMap();
    for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
      resp.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    responder.sendJson(resp);
  }
}
