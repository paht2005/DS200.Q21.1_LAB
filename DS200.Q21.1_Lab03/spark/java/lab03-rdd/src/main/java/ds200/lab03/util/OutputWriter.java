package ds200.lab03.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class OutputWriter {
  private OutputWriter() {
  }

  public static void writeLines(String outputPath, List<String> lines) throws IOException {
    Path path = Path.of(outputPath);
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.write(path, lines, StandardCharsets.UTF_8);
  }
}