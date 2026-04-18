package ds200.lab03.model;

import java.io.Serializable;

public final class Rating implements Serializable {
  private final int userId;
  private final int movieId;
  private final double rating;
  private final long timestamp;

  public Rating(int userId, int movieId, double rating, long timestamp) {
    this.userId = userId;
    this.movieId = movieId;
    this.rating = rating;
    this.timestamp = timestamp;
  }

  public int getUserId() {
    return userId;
  }

  public int getMovieId() {
    return movieId;
  }

  public double getRating() {
    return rating;
  }

  public long getTimestamp() {
    return timestamp;
  }
}