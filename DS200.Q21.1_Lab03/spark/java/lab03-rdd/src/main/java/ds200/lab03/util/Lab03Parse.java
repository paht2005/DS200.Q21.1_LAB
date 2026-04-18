package ds200.lab03.util;

import ds200.lab03.model.Movie;
import ds200.lab03.model.Rating;
import ds200.lab03.model.User;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class Lab03Parse {
  private Lab03Parse() {
  }

  public static Movie parseMovie(String line) {
    if (line == null || line.isBlank()) {
      return null;
    }
    String[] parts = line.split(",", 3);
    if (parts.length < 3) {
      return null;
    }
    try {
      int movieId = Integer.parseInt(parts[0].trim());
      String title = parts[1].trim();
      List<String> genres = Arrays.stream(parts[2].split("\\|"))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList());
      return new Movie(movieId, title, genres);
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  public static Rating parseRating(String line) {
    if (line == null || line.isBlank()) {
      return null;
    }
    String[] parts = line.split(",", 4);
    if (parts.length < 4) {
      return null;
    }
    try {
      int userId = Integer.parseInt(parts[0].trim());
      int movieId = Integer.parseInt(parts[1].trim());
      double rating = Double.parseDouble(parts[2].trim());
      long timestamp = Long.parseLong(parts[3].trim());
      return new Rating(userId, movieId, rating, timestamp);
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  public static User parseUser(String line) {
    if (line == null || line.isBlank()) {
      return null;
    }
    String[] parts = line.split(",", 5);
    if (parts.length < 5) {
      return null;
    }
    try {
      int userId = Integer.parseInt(parts[0].trim());
      String gender = parts[1].trim();
      int age = Integer.parseInt(parts[2].trim());
      int occupationId = Integer.parseInt(parts[3].trim());
      return new User(userId, gender, age, occupationId);
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  public static Map.Entry<Integer, String> parseOccupation(String line) {
    if (line == null || line.isBlank()) {
      return null;
    }
    String[] parts = line.split(",", 2);
    if (parts.length < 2) {
      return null;
    }
    try {
      int occupationId = Integer.parseInt(parts[0].trim());
      String occupationName = parts[1].trim();
      return new AbstractMap.SimpleEntry<>(occupationId, occupationName);
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  public static String ageGroup(int age) {
    if (age <= 18) {
      return "0-18";
    }
    if (age <= 35) {
      return "19-35";
    }
    if (age <= 50) {
      return "36-50";
    }
    return "51+";
  }

  public static int yearFromTimestamp(long timestamp) {
    return Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC).getYear();
  }

  public static String fmt(double value) {
    return String.format(Locale.US, "%.4f", value);
  }
}