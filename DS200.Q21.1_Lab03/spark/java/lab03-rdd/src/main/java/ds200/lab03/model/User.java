package ds200.lab03.model;

import java.io.Serializable;

public final class User implements Serializable {
  private final int userId;
  private final String gender;
  private final int age;
  private final int occupationId;

  public User(int userId, String gender, int age, int occupationId) {
    this.userId = userId;
    this.gender = gender;
    this.age = age;
    this.occupationId = occupationId;
  }

  public int getUserId() {
    return userId;
  }

  public String getGender() {
    return gender;
  }

  public int getAge() {
    return age;
  }

  public int getOccupationId() {
    return occupationId;
  }
}