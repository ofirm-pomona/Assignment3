package com.miron.assignment3;

class Movie {

    private String id;
    private float rating;
    private String name;

    Movie(String id, float rating) {
        this.id = id;
        this.rating = rating;
    }

    String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    float getRating() {
        return rating;
    }

    public void setRating(float rating) {
        this.rating = rating;
    }

    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "id='" + id + '\'' +
                ", rating=" + rating +
                ", name='" + name + '\'' +
                '}';
    }
}
