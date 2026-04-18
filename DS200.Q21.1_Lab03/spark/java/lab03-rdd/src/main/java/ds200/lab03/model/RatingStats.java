package ds200.lab03.model;

import java.io.Serializable;

public final class RatingStats implements Serializable {
  private final double sum;
  private final long count;

  public RatingStats(double sum, long count) {
    this.sum = sum;
    this.count = count;
  }

  public double getSum() {
    return sum;
  }

  public long getCount() {
    return count;
  }

  public double getAverage() {
    return count == 0 ? 0.0 : sum / count;
  }

  public static RatingStats merge(RatingStats left, RatingStats right) {
    return new RatingStats(left.sum + right.sum, left.count + right.count);
  }
}