package com.oracle.cdap.apps.sentiment;

public class Tweet {
  private String text;
  private String sentiment;
  private long createdAt;

  public Tweet(String text, long createdAt) {
    this.text = text;
    this.createdAt = createdAt;
  }
  public void setSentiment(String sentiment) {
    this.sentiment = sentiment;
  }
  public String getText() {
    return text;
  }
  public String getSentiment() {
    return sentiment;
  }
  public long getCreatedAt() {
    return createdAt;
  }
}
