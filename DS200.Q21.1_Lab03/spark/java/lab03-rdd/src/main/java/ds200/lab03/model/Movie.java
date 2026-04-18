package ds200.lab03.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Movie implements Serializable {
  private final int movieId;
  private final String title;
  private final List<String> genres;

  public Movie(int movieId, String title, List<String> genres) {
    this.movieId = movieId;
    this.title = title;
    this.genres = Collections.unmodifiableList(new ArrayList<>(genres));
  }

  public int getMovieId() {
    return movieId;
  }

  public String getTitle() {
    return title;
  }

  public List<String> getGenres() {
    return genres;
  }
}