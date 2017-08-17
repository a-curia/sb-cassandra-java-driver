package com.dbbyte.repository;


import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.dbbyte.domain.Movie;

public class MovieRepository {

    private static final String TABLE_NAME = "movies";

    private static final String TABLE_NAME_BY_TITLE = TABLE_NAME + "ByTitle";

    private Session session;

    public MovieRepository(Session session) {
        this.session = session;
    }

    
    /**
     * Creates the movies table.
     */
    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME).append("(").append("id uuid PRIMARY KEY, ").append("title text,").append("author text,").append("subject text);");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Creates the movies table.
     */
    public void createTableMoviesByTitle() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME_BY_TITLE).append("(").append("id uuid, ").append("title text,").append("PRIMARY KEY (title, id));");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Alters the table movies and adds an extra column.
     */
    public void alterTablemovies(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ").append(TABLE_NAME).append(" ADD ").append(columnName).append(" ").append(columnType).append(";");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a row in the table movies. 
     * 
     * @param movie
     */
    public void insertmovie(Movie movie) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(id, title, author, subject) ").append("VALUES (").append(movie.getId()).append(", '").append(movie.getTitle()).append("', '").append(movie.getRegizor()).append("', '")
                .append(movie.getGenre()).append("');");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a row in the table moviesByTitle.
     * @param movie
     */
    public void insertmovieByTitle(Movie movie) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(movie.getId()).append(", '").append(movie.getTitle()).append("');");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a movie into two identical tables using a batch query.
     * 
     * @param movie
     */
    public void insertMovieBatch(Movie movie) {
        StringBuilder sb = new StringBuilder("BEGIN BATCH ").append("INSERT INTO ").append(TABLE_NAME).append("(id, title, author, subject) ").append("VALUES (").append(movie.getId()).append(", '").append(movie.getTitle()).append("', '").append(movie.getRegizor())
                .append("', '").append(movie.getGenre()).append("');").append("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(movie.getId()).append(", '").append(movie.getTitle()).append("');")
                .append("APPLY BATCH;");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Select movie by id.
     * 
     * @return
     */
    public Movie selectByTitle(String title) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_TITLE).append(" WHERE title = '").append(title).append("';");

        final String query = sb.toString();

        ResultSet rs = session.execute(query);

        List<Movie> movies = new ArrayList<Movie>();

        for (Row r : rs) {
            Movie s = new Movie(r.getUUID("id"), r.getString("title"), null, null);
            movies.add(s);
        }

        return movies.get(0);
    }

    /**
     * Select all movies from movies
     * 
     * @return
     */
    public List<Movie> selectAll() {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Movie> movies = new ArrayList<Movie>();

        for (Row r : rs) {
            Movie movie = new Movie(r.getUUID("id"), r.getString("title"), r.getString("author"), r.getString("subject"));
            movies.add(movie);
        }
        return movies;
    }

    /**
     * Select all movies from moviesByTitle
     * @return
     */
    public List<Movie> selectAllMovieByTitle() {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_TITLE);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Movie> movies = new ArrayList<Movie>();

        for (Row r : rs) {
            Movie movie = new Movie(r.getUUID("id"), r.getString("title"), null, null);
            movies.add(movie);
        }
        return movies;
    }

    /**
     * Delete a movie by title.
     */
    public void deletemovieByTitle(String title) {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(TABLE_NAME_BY_TITLE).append(" WHERE title = '").append(title).append("';");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Delete table.
     * 
     * @param tableName the name of the table to delete.
     */
    public void deleteTable(String tableName) {
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);

        final String query = sb.toString();
        session.execute(query);
    }
}