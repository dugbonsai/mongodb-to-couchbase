import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.Console;
import java.util.*;

import static com.couchbase.client.java.kv.MutateInSpec.replace;
import static com.couchbase.client.java.kv.MutateInSpec.upsert;

public class CouchbaseTest {
    public static void main(String... args) {
        //
        // Connect to Couchbase Servre
        //

        // Connect to cluster on localhost
        Cluster cluster = Cluster.connect("127.0.0.1", "mflix_client", "password");
        ReactiveCluster reactiveCluster = cluster.reactive();

        // Get sample_mflix bucket reference
        Bucket bucket = cluster.bucket("sample_mflix");
        ReactiveBucket reactiveBucket = bucket.reactive();

        // Get default collection reference
        Collection collection = bucket.defaultCollection();
        ReactiveCollection reactiveCollection = collection.reactive();

        //
        // Retrieve a Document by ID
        //
        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579cc").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579cc not found!");
        }

        try {
            System.out.println(collection.get("movie:573a1390f29313caabcd4135").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document movie:573a1390f29313caabcd4135 not found!");
        }

        //
        // Insert a New Document
        //

        // Create new JsonObject to insert
        JsonObject doc = JsonObject.create()
                .put("name", "Anat Chase")
                .put("email", "anat_chase@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd4135")
                .put("text", "This is Anat's review")
                .put("type", "comment");

        // Insert the new document
        try {
            collection.insert("comment:5a9427648b0beebeb69579c0", doc);
        } catch (DocumentExistsException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c0 already exists!");
        } catch (CouchbaseException ex) {
            System.err.println("Something else happened: " + ex);
        }

        //
        // Insert Multiple New Documents
        //

        // Generate two JSON documents
        List<Tuple2<String, JsonObject>> documents = new ArrayList<Tuple2<String, JsonObject>>();
        doc = JsonObject.create()
                .put("name", "Anat Chase")
                .put("email", "anat_chase@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd42e8")
                .put("text", "This is Anat's review")
                .put("type", "comment");
        documents.add(Tuples.of("comment:5a9427648b0beebeb69579c1", doc));

        JsonObject doc2 = JsonObject.create()
                .put("name", "Anat Chase")
                .put("email", "anat_chase@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd4323")
                .put("text", "This is Anat's review")
                .put("type", "comment");
        documents.add(Tuples.of("comment:5a9427648b0beebeb69579c2", doc2));

        // Insert the 2 documents in one batch, waiting until the last one is done.
        // insert() will throw an exception if a document with the specified ID already exists
        try {
            Flux
                    .fromIterable(documents)
                    .parallel().runOn(Schedulers.elastic())
                    .concatMap(doc3 -> reactiveCollection.insert(doc3.getT1(), doc3.getT2())
                            .onErrorResume(e -> Mono.error(new Exception(doc3.getT1(), e))))
                    .sequential().collectList().block();
        } catch (Exception ex) {
            System.err.println("Document " + ex.getMessage() + " already exists!");
        }

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c0").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c0 not found!");
        }

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c1").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c1 not found!");
        }

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c2").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c2 not found!");
        }

        //
        // Update an Existing Document
        //

        // Update a document using the sub-document API to modify the specific attribute(s)
        // replace() will throw an exception if a document with the specified ID does not exist
        try {
            collection.mutateIn("comment:5a9427648b0beebeb69579c0",
                    Arrays.asList(replace("text", "This is not Anat's review")));
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c0 not found!");
        } catch (PathNotFoundException ex) {
            System.err.println("Path 'text' not found in comment:5a9427648b0beebeb69579c0");
        }

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c0").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c0 not found!");
        }

        //
        // Update Multiple Documents
        //

        // execute a N1QL UPDATE query via the query API
        // CREATE INDEX idx1 on sample_mflix(name) WHERE type='comment'
        try {
            String statement =
                    "UPDATE sample_mflix " +
                            "SET name='Anita Chase', email='anita_chase@fakegmail.com' " +
                            "WHERE type='comment' AND name='Anat Chase'";
            QueryResult updateResult = cluster.query(statement);
        } catch (CouchbaseException ex) {
            System.err.println("UPDATE failed: " + ex.getMessage());
        }

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c0").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c0 not found!");
        }

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c1").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c1 not found!");
        }

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c2").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c2 not found!");
        }

        //
        // Update or Insert a Document
        //
        doc = JsonObject.create()
                .put("name", "Mia Hannas")
                .put("email", "mia_hannas@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd4135")
                .put("text", "This is Mia's review")
                .put("type", "comment");

        // upsert() will update the document if it exists or insert the document if it does not exist
        collection.upsert("comment:5a9427648b0beebeb69579c0", doc);

        try {
            System.out.println(collection.get("comment:5a9427648b0beebeb69579c0").contentAsObject());
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c0 not found!");
        }


        //
        // Delete a document
        //

        // remove() will throw an exception if the document does not exist
        try {
            collection.remove("comment:5a9427648b0beebeb69579c0");
        } catch (DocumentNotFoundException ex) {
            System.err.println("Document comment:5a9427648b0beebeb69579c0 not found!");
        }

        //
        // Delete Multiple Documents
        //

        // execute a N1QL DELETE query via the query API
        try {
            String statement = "DELETE FROM sample_mflix " +
                "WHERE type='comment' AND name='Anita Chase'";

            QueryResult deleteResult = cluster.query(statement);
        } catch (CouchbaseException ex) {
            System.err.println("DELETE failed: " + ex.getMessage());
        }

        //
        // Data Access with N1QL
        //

        // execute a N1QL SELECT query (blocking) via the query API
        // CREATE INDEX idx2 on sample_mflix(year, imdb.rating, title) WHERE type="movie"
        try {
            String selectStatement = "SELECT title, year, imdb.rating FROM sample_mflix " +
                    "WHERE type='movie' AND year BETWEEN 1970 AND 1979 ORDER BY imdb.rating DESC";
            final QueryResult selectResult = cluster.query(selectStatement);

            for (JsonObject row : selectResult.rowsAsObject()) {
                System.out.println(row.toString());
            }
        } catch (CouchbaseException ex) {
            System.err.println("SELECT failed: " + ex.getMessage());
        }
    }
}
