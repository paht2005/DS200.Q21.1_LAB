package ds200.lab01;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * CSV parsing and report helpers for Lab 01 (comma-separated export, same rules as the Python reference).
 */
public final class Lab01Parse {

  private Lab01Parse() {}

  /** Split at most {@code maxSplits} commas (same idea as Python {@code split(",", maxSplits)}). */
  private static String[] splitCsv(String line, int maxSplits) {
    List<String> parts = new ArrayList<>();
    int start = 0;
    int splits = 0;
    while (splits < maxSplits) {
      int idx = line.indexOf(',', start);
      if (idx < 0) {
        parts.add(line.substring(start).trim());
        return parts.toArray(new String[0]);
      }
      parts.add(line.substring(start, idx).trim());
      start = idx + 1;
      splits++;
    }
    parts.add(line.substring(start).trim());
    return parts.toArray(new String[0]);
  }

  /**
   * Parse a movies.txt line: MovieID, Title, Genres.
   *
   * @return [movieId, title, genresRaw] or null if invalid
   */
  public static String[] parseMovieLine(String line) {
    if (line == null) {
      return null;
    }
    line = line.trim();
    if (line.isEmpty()) {
      return null;
    }
    String[] p = splitCsv(line, 2);
    if (p.length < 3) {
      return null;
    }
    return p;
  }

  /**
   * Parse a ratings line: UserID, MovieID, Rating, Timestamp.
   *
   * @return [userId, movieId, rating] or null
   */
  public static String[] parseRatingLine(String line) {
    if (line == null) {
      return null;
    }
    line = line.trim();
    if (line.isEmpty()) {
      return null;
    }
    String[] p = splitCsv(line, 3);
    if (p.length < 3) {
      return null;
    }
    try {
      Double.parseDouble(p[2]);
    } catch (NumberFormatException e) {
      return null;
    }
    return new String[] {p[0], p[1], p[2]};
  }

  /**
   * Parse users.txt: UserID, Gender, Age, Occupation, Zip-code.
   *
   * @return [userId, genderUpper, ageString] or null
   */
  public static String[] parseUserLine(String line) {
    if (line == null) {
      return null;
    }
    line = line.trim();
    if (line.isEmpty()) {
      return null;
    }
    String[] p = splitCsv(line, 4);
    if (p.length < 3) {
      return null;
    }
    try {
      Integer.parseInt(p[2]);
    } catch (NumberFormatException e) {
      return null;
    }
    return new String[] {p[0], p[1].toUpperCase(Locale.ROOT), p[2]};
  }

  /** Disjoint age buckets (18 belongs to 0-18, same as Python {@code age_bucket}). */
  public static String ageBucket(int age) {
    if (age <= 18) {
      return "0-18";
    }
    if (age <= 35) {
      return "18-35";
    }
    if (age <= 50) {
      return "35-50";
    }
    return "50+";
  }

  /** Trim trailing zeros from a fixed four-decimal string (matches Python {@code fmt_rating}). */
  public static String fmtRating(double x) {
    String s = String.format(Locale.ROOT, "%.4f", x);
    s = s.replaceAll("0+$", "");
    s = s.replaceAll("\\.$", "");
    return s.isEmpty() ? "0" : s;
  }

  /** Read all text lines from a stream using the given charset (side tables: ISO-8859-1 per lab notes). */
  public static List<String> readAllLines(java.io.InputStream in, java.nio.charset.Charset cs)
      throws java.io.IOException {
    List<String> lines = new ArrayList<>();
    try (java.io.BufferedReader br =
        new java.io.BufferedReader(new java.io.InputStreamReader(in, cs))) {
      String ln;
      while ((ln = br.readLine()) != null) {
        lines.add(ln);
      }
    }
    return lines;
  }

  public static java.nio.charset.Charset sideFileCharset() {
    return StandardCharsets.ISO_8859_1;
  }
}
