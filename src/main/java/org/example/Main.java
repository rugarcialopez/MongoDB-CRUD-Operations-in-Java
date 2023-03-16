package org.example;

import static com.mongodb.client.model.Filters.*;

import com.mongodb.client.*;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb+srv://ruben:1gECt3WUfdj1kTFW@cluster0.foufqjr.mongodb.net/?retryWrites=true&w=majority");
        MongoDatabase db = client.getDatabase("sample_analytics");
        MongoCollection collection = db.getCollection("accounts");
        FindIterable<Document> documents = collection.find(eq("_id", new ObjectId("5ca4bbc7a2dd94ee5816238e")));
        MongoCursor<Document> cursor = documents.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        // Find using criteria
        documents = collection.find(and(gte("balance", 1000),eq("account_type","checking")));
        cursor = documents.iterator();
        {
            while(cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }

        // Get first
        Document doc = (Document) collection.find(and(gte("balance", 1000), eq("account_type", "checking"))).first();
        System.out.println(doc.toJson());

        // Insert One
        List products = new ArrayList();
        products.add("banana");
        products.add("apple");
        Document document = new Document("_id", new ObjectId())
                .append("account_id", 3333)
                .append("limit", 5555)
                .append("products", products);
        InsertOneResult result = collection.insertOne(document);
        BsonValue id = result.getInsertedId();
        System.out.println(id);

        // Insert One
        Document inspection = new Document("_id", new ObjectId())
                .append("id", "10021-2015-ENFO")
                .append("certificate_number", 9278806)
                .append("business_name", "ATLIXCO DELI GROCERY INC.")
                .append("date", Date.from(LocalDate.of(2015, 2, 20).atStartOfDay(ZoneId.systemDefault()).toInstant()))
                .append("result", "No Violation Issued")
                .append("sector", "Cigarette Retail Dealer - 127")
                .append("address", new Document().append("city", "RIDGEWOOD").append("zip", 11385).append("street", "MENAHAN ST").append("number", 1712));
        InsertOneResult result2 = collection.insertOne(inspection);
        BsonValue id2 = result2.getInsertedId();
        System.out.println(id2);

        //Insert Many
        Document doc1 = new Document().append("account_holder","john doe").append("account_id","MDB99115881").append("balance",1785).append("account_type","checking");
        Document doc2 = new Document().append("account_holder","jane doe").append("account_id","MDB79101843").append("balance",1468).append("account_type","checking");
        List<Document> accounts = Arrays.asList(doc1, doc2);
        InsertManyResult result3 = collection.insertMany(accounts);
        result3.getInsertedIds().forEach((x,y)-> System.out.println(y.asObjectId()));

        // Unpdate One
        Bson query  = eq("account_id","MDB12234728");
        Bson updates  = Updates.combine(Updates.set("account_status","active"),Updates.inc("balance",100));
        UpdateResult upResult = collection.updateOne(query, updates);

        // Update Many
        query  = eq("account_type","savings");
        updates  = Updates.combine(Updates.set("minimum_balance",100));
        upResult = collection.updateMany(query, updates);

        // Delete One
        query = eq("account_holder", "john doe");
        DeleteResult delResult = collection.deleteOne(query);
        System.out.println("Deleted a document:");
        System.out.println("\t" + delResult.getDeletedCount());

        // Using Delete Many with a Query Object
        query = eq("account_status", "dormant");
        delResult = collection.deleteMany(query);
        System.out.println(delResult.getDeletedCount());

        // Using deleteMany() with a Query Filter
        delResult = collection.deleteMany(eq("account_status", "dormant"));
        System.out.println(delResult.getDeletedCount());


    }
}