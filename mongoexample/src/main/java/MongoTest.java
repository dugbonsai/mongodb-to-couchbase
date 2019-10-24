import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class MongoTest {
    public static void main(String[] args) {
        // Connect to database
        MongoClient mongoClient =
                MongoClients.create("mongodb+srv://admin:adm1npassw0rd@dbcluster-vlxk2.mongodb.net/test?retryWrites=true");
        MongoDatabase mongoDatabase = mongoClient.getDatabase("sample_mflix");
        MongoCollection<Document> comments = mongoDatabase.getCollection("comments");
        MongoCollection<Document> movies = mongoDatabase.getCollection("movies");

        // Get a document by ID
        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579cc"))).forEach(printBlock);
        movies.find(Filters.eq("_id", new ObjectId("573a1390f29313caabcd4135"))).forEach(printBlock);

        // Insert a new document
        Document doc = new Document("_id", new ObjectId("5a9427648b0beebeb69579c0"))
                .append("name", "Anat Chase")
                .append("email", "anat_chase@fakegmail.com")
                .append("movie_id", new ObjectId("573a1390f29313caabcd4135"))
                .append("text", "This is Anat's review");

        comments.insertOne(doc);

        // Insert multiple documents
        List<Document> documents = new ArrayList<Document>();
        Document doc1 = new Document("_id", new ObjectId("5a9427648b0beebeb69579c1"))
                .append("name", "Anat Chase")
                .append("email", "anat_chase@fakegmail.com")
                .append("movie_id", new ObjectId("573a1390f29313caabcd42e8"))
                .append("text", "This is Anat's review");
        documents.add(doc1);

        Document doc2 = new Document("_id", new ObjectId("5a9427648b0beebeb69579c2"))
                .append("name", "Anat Chase")
                .append("email", "anat_chase@fakegmail.com")
                .append("movie_id", new ObjectId("573a1390f29313caabcd4323"))
                .append("text", "This is Anat's review");
        documents.add(doc2);
        comments.insertMany(documents);

        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c0"))).forEach(printBlock);
        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c1"))).forEach(printBlock);
        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c2"))).forEach(printBlock);

        // Update a document
        comments.updateOne(
                Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c0")),
                Updates.combine(Updates.set("text", "")));

        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c0"))).forEach(printBlock);

        // Update multiple documents
        comments.updateMany(
                Filters.eq("name", "Anat Chase"),
                Updates.combine(
                        Updates.set("name", "Anita Chase"),
                        Updates.set("email", "anita_chase@fakegmail.com")));

        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c0"))).forEach(printBlock);
        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c1"))).forEach(printBlock);
        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c2"))).forEach(printBlock);

        // Replace a document
        comments.replaceOne(
                Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c0")),
                new Document("name", "Mia Hannas")
                        .append("email", "mia_hannas@fakegmail.com")
                        .append("movie_id", new ObjectId("573a1390f29313caabcd4135"))
                        .append("text", "This is Mia's review"),
                new UpdateOptions().upsert(true));

        comments.find(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c0"))).forEach(printBlock);

        // Delete a document
        comments.deleteOne(Filters.eq("_id", new ObjectId("5a9427648b0beebeb69579c0")));

        // Delete multiple documents
        comments.deleteMany(Filters.eq("name", "Anita Chase"));

        // Query a collection
        movies.find(Filters.and(Filters.gte("year", 1970), Filters.lte("year", 1979)))
                .sort(Sorts.descending("imdb.rating"))
                .projection(Projections.fields(
                        Projections.include("title", "year", "imdb.rating" ),
                        Projections.excludeId()))
                .forEach(printBlock);
    }

    static Block<Document> printBlock = new Block<Document>() {
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };
}
