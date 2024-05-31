package org.example;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typedb.driver.api.TypeDBCredential;
import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;

import javax.lang.model.type.NullType;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Example {
    public static void loadBatch(TypeDBSession session, List<String> batch) {
        try (TypeDBTransaction transaction = session.transaction(TypeDBTransaction.Type.WRITE)) {
            for (String query: batch) {
                transaction.query().insert(query);
            }
            transaction.commit();
        }
    }

    public static void loadData(Set<String> addresses, String username, String password) throws FileNotFoundException {
        String database = "bookstore";
        ArrayList<String> dataFiles = new ArrayList<>(List.of("contributors.tql", "publishers.tql", "books.tql"));
        int batchSize = 100;
        TypeDBCredential credential = new TypeDBCredential(username, password, true);

        try (TypeDBDriver driver = TypeDB.cloudDriver(addresses, credential)) {
            try (TypeDBSession session = driver.session(database, TypeDBSession.Type.DATA)) {
                BatchIterator batchIterator = new BatchIterator(dataFiles, batchSize);

                while (batchIterator.hasNext()) {
                    List<String> batch = batchIterator.next();
                    loadBatch(session, batch);
                }
            }
        }
    }

    public static void addBatches(
        ArrayList<String> filepaths,
        int batchSize,
        LinkedBlockingQueue<Either<List<String>, NullType>> queue,
        AtomicBoolean hasError
    ) throws InterruptedException, FileNotFoundException {
        BatchIterator batchIterator = new BatchIterator(filepaths, batchSize);

        while (batchIterator.hasNext() && !hasError.get()) {
            List<String> batch = batchIterator.next();
            queue.put(Either.first(batch));
        }
    }

    public static CompletableFuture<Void> scheduleBatchLoader(
        ExecutorService executor,
        LinkedBlockingQueue<Either<List<String>, NullType>> queue,
        TypeDBSession session,
        AtomicBoolean hasError
    ) {
        return CompletableFuture.runAsync(() -> {
            Either<List<String>, NullType> queries;
            try {
                while ((queries = queue.take()).isFirst() && !hasError.get()) {
                    try (TypeDBTransaction transaction = session.transaction(TypeDBTransaction.Type.WRITE)) {
                        for (String query: queries.first()) {
                            transaction.query().insert(query);
                        }
                        transaction.commit();
                    }
                }
            } catch (Throwable e) {
                hasError.set(true);
                throw new RuntimeException(e);
            }
        }, executor);
    }

    public static void loadDataAsync(Set<String> addresses, String username, String password) throws InterruptedException, FileNotFoundException {
        String database = "bookstore";
        ArrayList<String> dataFiles = new ArrayList<>(List.of("contributors.tql", "publishers.tql", "books.tql"));
        int batchSize = 100;
        TypeDBCredential credential = new TypeDBCredential(username, password, true);

        try (TypeDBDriver driver = TypeDB.cloudDriver(addresses, credential)) {
            try (TypeDBSession session = driver.session(database, TypeDBSession.Type.DATA)) {
                for (String dataFile: dataFiles) {
                    int poolSize = Runtime.getRuntime().availableProcessors();
                    ExecutorService executor = Executors.newFixedThreadPool(poolSize, new NamedThreadFactory(database));
                    LinkedBlockingQueue<Either<List<String>, NullType>> queue = new LinkedBlockingQueue<>(4 * poolSize);
                    List<CompletableFuture<Void>> batchLoadersFutures = new ArrayList<>(poolSize);
                    AtomicBoolean hasError = new AtomicBoolean(false);

                    for (int i = 0; i < poolSize; i++) {
                        batchLoadersFutures.add(scheduleBatchLoader(executor, queue, session, hasError));
                    }

                    addBatches(new ArrayList<>(List.of(dataFile)), batchSize, queue, hasError);

                    for (int i = 0; i < poolSize; i++) {
                        queue.put(Either.second(null));
                    }

                    CompletableFuture.allOf(batchLoadersFutures.toArray(new CompletableFuture[0])).join();
                    executor.shutdown();
                }
            }
        }
    }
}
