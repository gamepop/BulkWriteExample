/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongodb;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.skip;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.eq;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import org.bson.Document;
import org.bson.types.Decimal128;

/**
 * This Simple example shows , A MongoDB aggregation executing in batch to
 * calculate dividends for customers based on a crude algorithm (Total purchase = Price * Quantity , Dividend = TP * 0.010 and then
 * applying the same to 2M+ customers using bulkWrite API.
 */

public class App {
    private static final int BATCH_SIZE = 10000;
    private static final Decimal128 DIVIDEND_AMOUNT = Decimal128.parse("0.015");

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     */
    public static void main(final String[] args) {
        MongoClient mongoClient;
        String uri = "mongodb+srv://<username>:<password>@<Atlas Cluster>/test?retryWrites=true&w=majority";

        mongoClient = MongoClients.create(uri);

        // get handle to "test" database
        MongoDatabase database = mongoClient.getDatabase("test");

        // get a handle to the "customers" collection
        MongoCollection<Document> collection = database.getCollection("customers");

        long customers = collection.countDocuments();

        // Itegrate thorugh batches of customers, calculate dividends and collect and
        // bulk apply
        for (int i = 0; i < customers; i += BATCH_SIZE) {
            List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
            Instant start = Instant.now();

            // This aggregation, Calculates total purchase amout by a customer and then
            // calculates dividend
            collection.aggregate(Arrays.asList(skip(i), limit(BATCH_SIZE), unwind("$orders"),
                    group("$_id", sum("totalPrice", eq("$multiply", Arrays.asList("$orders.price", "$orders.qty")))),
                    addFields(new Field<Document>("dividend",
                            new Document("$multiply", Arrays.asList("$totalPrice", DIVIDEND_AMOUNT))))))
                    .forEach((Consumer<Document>) document -> {
                        writes.add(new UpdateOneModel<Document>(new Document("_id", document.get("_id")), // filter
                                new Document("$set",
                                        new Document("dividend", document.get("dividend", Decimal128.class)))));
                    });

            Instant end = Instant.now();
            System.out.println("Time to read batch for i = " + i + "  " + Duration.between(start, end));

            Instant writeStart = Instant.now();
            BulkWriteResult bulkWriteResult = collection.bulkWrite(writes, new BulkWriteOptions().ordered(false));
            Instant writeEnd = Instant.now();
            System.out.println("Time to write batch for i = " + i + "  " + Duration.between(writeStart, writeEnd));

            if (bulkWriteResult.getModifiedCount() > 0)
                System.out.println("Updated Documents : " + bulkWriteResult.getModifiedCount());
            continue;
        }

        // release resources
        mongoClient.close();
    }
}