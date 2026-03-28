package ds200.lab01;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Load small replicated tables (movies, users) from the distributed cache URI list.
 */
public final class SideTables {

  /** Gender (M/F) and age from users.txt. */
  public static final class UserRow {
    public final String gender;
    public final int age;

    public UserRow(String gender, int age) {
      this.gender = gender;
      this.age = age;
    }
  }

  private SideTables() {}

  private static boolean pathEndsWith(String path, String suffix) {
    if (path == null) {
      return false;
    }
    String n = path.replace('\\', '/');
    return n.endsWith(suffix);
  }

  public static Map<String, String> loadMovieTitles(Configuration conf, URI[] cacheFiles, String fileName)
      throws IOException {
    Charset cs = Lab01Parse.sideFileCharset();
    Map<String, String> out = new HashMap<>();
    if (cacheFiles == null) {
      return out;
    }
    for (URI u : cacheFiles) {
      if (!pathEndsWith(u.getPath(), fileName)) {
        continue;
      }
      try (InputStream in = openUri(conf, u)) {
        for (String line : Lab01Parse.readAllLines(in, cs)) {
          String[] p = Lab01Parse.parseMovieLine(line);
          if (p != null) {
            out.put(p[0], p[1]);
          }
        }
      }
      return out;
    }
    return out;
  }

  /** MovieID -> list of genre names (split Genres on {@code |}). */
  public static Map<String, List<String>> loadGenresByMovie(
      Configuration conf, URI[] cacheFiles, String fileName) throws IOException {
    Charset cs = Lab01Parse.sideFileCharset();
    Map<String, List<String>> out = new HashMap<>();
    if (cacheFiles == null) {
      return out;
    }
    for (URI u : cacheFiles) {
      if (!pathEndsWith(u.getPath(), fileName)) {
        continue;
      }
      try (InputStream in = openUri(conf, u)) {
        for (String line : Lab01Parse.readAllLines(in, cs)) {
          String[] p = Lab01Parse.parseMovieLine(line);
          if (p == null) {
            continue;
          }
          String mid = p[0];
          String genres = p[2];
          List<String> glist = new ArrayList<>();
          for (String g : genres.split("\\|")) {
            String t = g.trim();
            if (!t.isEmpty()) {
              glist.add(t);
            }
          }
          out.put(mid, glist);
        }
      }
      return out;
    }
    return out;
  }

  public static Map<String, UserRow> loadUsers(Configuration conf, URI[] cacheFiles, String fileName)
      throws IOException {
    Charset cs = Lab01Parse.sideFileCharset();
    Map<String, UserRow> out = new HashMap<>();
    if (cacheFiles == null) {
      return out;
    }
    for (URI u : cacheFiles) {
      if (!pathEndsWith(u.getPath(), fileName)) {
        continue;
      }
      try (InputStream in = openUri(conf, u)) {
        for (String line : Lab01Parse.readAllLines(in, cs)) {
          String[] p = Lab01Parse.parseUserLine(line);
          if (p == null) {
            continue;
          }
          int age = Integer.parseInt(p[2]);
          out.put(p[0], new UserRow(p[1], age));
        }
      }
      return out;
    }
    return out;
  }

  /** Open a path for the scheme implied by {@code u} ({@code file://} locally, {@code hdfs://} on cluster). */
  private static InputStream openUri(Configuration conf, URI u) throws IOException {
    Path path = new Path(u);
    FileSystem fs = FileSystem.get(u, conf);
    return fs.open(path);
  }
}
