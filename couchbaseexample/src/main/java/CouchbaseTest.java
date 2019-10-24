import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.subdoc.DocumentFragment;
import rx.Observable;

import java.io.Console;
import java.util.ArrayList;
// import java.util.Arrays;
import java.util.List;
/*
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
*/

public class CouchbaseTest {
    public static void main(String... args) {
/*
        Logger logger = Logger.getLogger("com.couchbase.client");
        for(Handler h : logger.getParent().getHandlers()) {
            if(h instanceof ConsoleHandler){
                h.setLevel(Level.FINE);
            }
        }

*/
        // Connect to cluster on localhost
        Cluster cluster = CouchbaseCluster.create("localhost"/*, specify additional nodes in cluster */);

        // Authenticate
        cluster.authenticate("mflix_client", "password");

        // Open the "sample_mflix" bucket
        Bucket bucket = cluster.openBucket("sample_mflix");

        // Get a document by ID
        System.out.println(bucket.get("comment:5a9427648b0beebeb69579cc"));
        System.out.println(bucket.get("movie:573a1390f29313caabcd4135"));

        // Create new JsonObject to insert
        JsonObject doc = JsonObject.create()
                .put("name", "Anat Chase")
                .put("email", "anat_chase@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd4135")
                .put("text", "This is Anat's review")
                .put("type", "comment");

        // Create a new JsonDocument from the JsonObject
        JsonDocument document = JsonDocument.create("comment:5a9427648b0beebeb69579c0", doc);

        // Insert the new document
        // insert() will throw an exception if a document with the specified ID already exists
        bucket.insert(document);

        // Insert multiple documents
        // Generate two JSON documents
        List<JsonDocument> documents = new ArrayList<JsonDocument>();
        doc = JsonObject.create()
                .put("name", "Anat Chase")
                .put("email", "anat_chase@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd42e8")
                .put("text", "This is Anat's review")
                .put("type", "comment");
        documents.add(JsonDocument.create("comment:5a9427648b0beebeb69579c1", doc));

        JsonObject doc2 = JsonObject.create()
                .put("name", "Anat Chase")
                .put("email", "anat_chase@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd4323")
                .put("text", "This is Anat's review")
                .put("type", "comment");
        documents.add(JsonDocument.create("comment:5a9427648b0beebeb69579c2", doc2));

        // Insert them in one batch, waiting until the last one is done.
        // insert() will throw an exception if a document with the specified ID already exists
        Observable
                .from(documents)
                .flatMap(doc3 -> bucket.async().insert(doc3))
                .last()
                .toBlocking()
                .single();

        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c0"));
        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c1"));
        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c2"));

        // Update a document using the sub-document API to modify the specific attribute(s)
        // replace() will throw an exception if a document with the specified ID does not exist
        DocumentFragment<Mutation> result =
                bucket.mutateIn("comment:5a9427648b0beebeb69579c0")
                        .replace("text", "This is not Anat's review")
                        .execute();

        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c0"));

        // Update multiple documents using a N1QL query
        // execute a N1QL UPDATE query via the query API
        // CREATE INDEX idx1 on sample_mflix(name) WHERE type='comment'
        String statement =
                "UPDATE sample_mflix " +
                "SET name='Anita Chase', email='anita_chase@fakegmail.com' " +
                "WHERE type='comment' AND name='Anat Chase'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(statement));

        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c0"));
        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c1"));
        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c2"));

        // Replace a document
        doc = JsonObject.create()
                .put("name", "Mia Hannas")
                .put("email", "mia_hannas@fakegmail.com")
                .put("movie_id", "movie:573a1390f29313caabcd4135")
                .put("text", "This is Mia's review")
                .put("type", "comment");

        // upsert() will update the document if it exists or insert the document if it does not exist
        bucket.upsert(JsonDocument.create("comment:5a9427648b0beebeb69579c0", doc));

        System.out.println(bucket.get("comment:5a9427648b0beebeb69579c0"));

        // Delete a document
        // remove() will throw an exception if the document does not exist
        bucket.remove("comment:5a9427648b0beebeb69579c0");

        // Delete multiple documents
        // execute a N1QL DELETE query via the query API
        statement = "DELETE FROM sample_mflix " +
                "WHERE type='comment' AND name='Anita Chase'";
        queryResult = bucket.query(N1qlQuery.simple(statement));

        // Query a collection
        // execute a N1QL SELECT query (synchronous) via the query API
        // CREATE INDEX idx2 on sample_mflix(year, imdb.rating, title) WHERE type="movie"
        statement = "SELECT title, year, imdb.rating FROM sample_mflix " +
                "WHERE type='movie' AND year BETWEEN 1970 AND 1979 ORDER BY imdb.rating DESC";
        queryResult = bucket.query(N1qlQuery.simple(statement));
        for(N1qlQueryRow row : queryResult) {
            System.out.println(row.value().toString());
        }
    }
}
