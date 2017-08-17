package com.dbbyte;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
import com.dbbyte.domain.Movie;
import com.dbbyte.repository.MovieRepository;
import com.dbbyte.repository.KeyspaceRepository;

public class MovieRepositoryIntegrationTest {

    private KeyspaceRepository schemaRepository;

    private MovieRepository movieRepository;

    private Session session;

    final String KEYSPACE_NAME = "testLibrary";
    final String MOVIES = "movies";
    final String MOVIES_BY_TITLE = "moviesByTitle";

    @BeforeClass
    public static void init() throws ConfigurationException, TTransportException, IOException, InterruptedException {
        // Start an embedded Cassandra Server
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000L);
    }

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9142);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
        schemaRepository.createKeyspace(KEYSPACE_NAME, "SimpleStrategy", 1);
        schemaRepository.useKeyspace(KEYSPACE_NAME);
        movieRepository = new MovieRepository(session);
    }

    @Test
    public void whenCreatingATable_thenCreatedCorrectly() {
        movieRepository.deleteTable(MOVIES);
        movieRepository.createTable();

        ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + MOVIES + ";");

        // Collect all the column names in one list.
        List<String> columnNames = result.getColumnDefinitions().asList().stream().map(cl -> cl.getName()).collect(Collectors.toList());
        assertEquals(columnNames.size(), 4);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("title"));
        assertTrue(columnNames.contains("author"));
        assertTrue(columnNames.contains("subject"));
    }

    @Test
    public void whenAlteringTable_thenAddedColumnExists() {
        movieRepository.deleteTable(MOVIES);
        movieRepository.createTable();

        movieRepository.alterTablemovies("publisher", "text");

        ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + MOVIES + ";");

        boolean columnExists = result.getColumnDefinitions().asList().stream().anyMatch(cl -> cl.getName().equals("publisher"));
        assertTrue(columnExists);
    }

    @Test
    public void whenAddingANewMovie_thenMovieExists() {
        movieRepository.deleteTable(MOVIES_BY_TITLE);
        movieRepository.createTableMoviesByTitle();

        String title = "Effective Java";
        String author = "Joshua Bloch";
        Movie movie = new Movie(UUIDs.timeBased(), title, author, "Programming");
        movieRepository.insertmovieByTitle(movie);

        Movie savedMovie = movieRepository.selectByTitle(title);
        assertEquals(movie.getTitle(), savedMovie.getTitle());
    }

    @Test
    public void whenAddingANewMovieBatch_ThenMovieAddedInAllTables() {
        // Create table movies
        movieRepository.deleteTable(MOVIES);
        movieRepository.createTable();

        // Create table moviesByTitle
        movieRepository.deleteTable(MOVIES_BY_TITLE);
        movieRepository.createTableMoviesByTitle();

        String title = "Effective Java";
        String author = "Joshua Bloch";
        Movie movie = new Movie(UUIDs.timeBased(), title, author, "Programming");
        movieRepository.insertMovieBatch(movie);

        List<Movie> movies = movieRepository.selectAll();

        assertEquals(1, movies.size());
        assertTrue(movies.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));

        List<Movie> moviesByTitle = movieRepository.selectAllMovieByTitle();

        assertEquals(1, moviesByTitle.size());
        assertTrue(moviesByTitle.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));
    }

    @Test
    public void whenSelectingAll_thenReturnAllRecords() {
        movieRepository.deleteTable(MOVIES);
        movieRepository.createTable();

        Movie movie = new Movie(UUIDs.timeBased(), "Effective Java", "Joshua Bloch", "Programming");
        movieRepository.insertmovie(movie);

        movie = new Movie(UUIDs.timeBased(), "Clean Code", "Robert C. Martin", "Programming");
        movieRepository.insertmovie(movie);

        List<Movie> movies = movieRepository.selectAll();

        assertEquals(2, movies.size());
        assertTrue(movies.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));
        assertTrue(movies.stream().anyMatch(b -> b.getTitle().equals("Clean Code")));
    }

    @Test
    public void whenDeletingAMovieByTitle_thenMovieIsDeleted() {
        movieRepository.deleteTable(MOVIES_BY_TITLE);
        movieRepository.createTableMoviesByTitle();

        Movie movie = new Movie(UUIDs.timeBased(), "Effective Java", "Joshua Bloch", "Programming");
        movieRepository.insertmovieByTitle(movie);

        movie = new Movie(UUIDs.timeBased(), "Clean Code", "Robert C. Martin", "Programming");
        movieRepository.insertmovieByTitle(movie);

        movieRepository.deletemovieByTitle("Clean Code");

        List<Movie> movies = movieRepository.selectAllMovieByTitle();
        assertEquals(1, movies.size());
        assertTrue(movies.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));
        assertFalse(movies.stream().anyMatch(b -> b.getTitle().equals("Clean Code")));

    }

    @Test(expected = InvalidQueryException.class)
    public void whenDeletingATable_thenUnconfiguredTable() {
        movieRepository.createTable();
        movieRepository.deleteTable(MOVIES);

        session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + MOVIES + ";");
    }

    @AfterClass
    public static void cleanup() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
}
