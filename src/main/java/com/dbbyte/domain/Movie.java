package com.dbbyte.domain;


import java.util.UUID;

public class Movie {

    private UUID id;

    private String title;

    private String regizor;

    private String genre;

    private String imdb;

    Movie() {
    }

    
    public Movie(UUID id, String title, String regizor, String genre) {
        this.id = id;
        this.title = title;
        this.regizor = regizor;
        this.genre = genre;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getRegizor() {
        return regizor;
    }

    public void setRegizor(String regizor) {
        this.regizor = regizor;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getImdb() {
        return imdb;
    }

    public void setImdb(String imdb) {
        this.imdb = imdb;
    }
}