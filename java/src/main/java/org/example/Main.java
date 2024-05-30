package org.example;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typedb.driver.api.TypeDBCredential;
import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;

import javax.lang.model.type.NullType;
import java.io.Console;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        ArrayList<String> filepaths = new ArrayList<>(List.of("../dataset/entities.tql", "../dataset/relations.tql"));
        int batchSize = 2;
        int transactionCount = 5;
        Set<String> addresses = Set.of("localhost:1729");
        String username = "admin";
        String password = "password";
        String database = "bulk-load-test";
        TypeDBCredential credential = new TypeDBCredential(username, password, true);
        String schema = Files.readString(Paths.get("../dataset/schema.tql"));

        try (TypeDBDriver driver = TypeDB.cloudDriver(addresses, credential)) {
            if (driver.databases().contains(database)) driver.databases().get(database).delete();
            driver.databases().create(database);

            try (TypeDBSession session = driver.session(database, TypeDBSession.Type.SCHEMA)) {
                try (TypeDBTransaction transaction = session.transaction(TypeDBTransaction.Type.WRITE)) {
                    transaction.query().define(schema);
                    transaction.commit();
                }
            }

            try (TypeDBSession session = driver.session(database, TypeDBSession.Type.DATA)) {
                PoolBulkLoader bulkLoader = new PoolBulkLoader(filepaths, batchSize, transactionCount, session);
                bulkLoader.load();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}