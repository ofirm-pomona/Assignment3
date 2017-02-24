package com.miron.assignment3;

class User {

    private String id;
    private int ratingCount;

    User(String id, int ratingCount) {
        this.id = id;
        this.ratingCount = ratingCount;
    }

    String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    int getRatingCount() {
        return ratingCount;
    }

    void setRatingCount(int ratingCount) {
        this.ratingCount = ratingCount;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", ratingCount=" + ratingCount +
                '}';
    }
}
