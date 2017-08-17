package com.dbbyte;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.dbbyte.domain.Movie;
import com.dbbyte.repository.MovieRepository;
import com.dbbyte.repository.KeyspaceRepository;

@SpringBootApplication
public class CassandraClient {
	
	private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);

	public static void main(String[] args) {
		SpringApplication.run(CassandraClient.class, args);
		
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", null);
        Session session = connector.getSession();

        KeyspaceRepository sr = new KeyspaceRepository(session);
        sr.createKeyspace("moviesimdb", "SimpleStrategy", 1);
        sr.useKeyspace("moviesimdb");

        MovieRepository br = new MovieRepository(session);
        br.createTable();
        br.alterTablemovies("publisher", "text");

        br.createTableMoviesByTitle();

        Movie movie = new Movie(UUIDs.timeBased(), "movie title", "regizor", "movie genre");
        br.insertMovieBatch(movie);

        br.selectAll().forEach(o -> LOG.info("Title in movies: " + o.getTitle()));
        br.selectAllMovieByTitle().forEach(o -> LOG.info("Title in moviesByTitle: " + o.getTitle()));

//        br.deletemovieByTitle("Effective Java");
//        br.deleteTable("movies");
//        br.deleteTable("moviesByTitle");

//        sr.deleteKeyspace("library");

        connector.close();		
	}
	
	
	
}
