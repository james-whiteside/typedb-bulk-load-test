package org.example;

import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class PoolBulkLoader {
    public final int transactionCount;
    public final int batchSize;
    private final AtomicBoolean hasError;
    private final TypeDBSession session;
    private final ArrayList<String> filepaths;


    public PoolBulkLoader(
        ArrayList<String> filepaths,
        int batchSize,
        int transactionCount,
        TypeDBSession session
    ) {
        this.filepaths = filepaths;
        this.batchSize = batchSize;
        this.transactionCount = transactionCount;
        this.session = session;
        this.hasError = new AtomicBoolean(false);
    }

    public void load() throws InterruptedException, FileNotFoundException {
        ExecutorService executor = Executors.newFixedThreadPool(transactionCount, new NamedThreadFactory(session.databaseName()));
        LinkedBlockingQueue<Either<List<String>, Done>> queue = new LinkedBlockingQueue<>(4 * transactionCount);
        List<CompletableFuture<Void>> batchLoadersFutures = new ArrayList<>(transactionCount);

        for (int i = 0; i < transactionCount; i++) {
            batchLoadersFutures.add(scheduleBatchLoader(executor, queue));
        }

        addBatches(queue);

        for (int i = 0; i < transactionCount; i++) {
            queue.put(Either.second(Done.INSTANCE));
        }

        CompletableFuture.allOf(batchLoadersFutures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
    }

    private void addBatches(LinkedBlockingQueue<Either<List<String>, Done>> queue) throws InterruptedException, FileNotFoundException {
        BatchIterator batchIterator = new BatchIterator(filepaths, batchSize);

        while (batchIterator.hasNext() && !hasError.get()) {
            List<String> batch = batchIterator.next();
            queue.put(Either.first(batch));
        }
    }

    private CompletableFuture<Void> scheduleBatchLoader(
        ExecutorService executor,
        LinkedBlockingQueue<Either<List<String>, Done>> queue
    ) {
        return CompletableFuture.runAsync(() -> {
            Either<List<String>, Done> queries;
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

    private static class Done {
        private static final Done INSTANCE = new Done();
    }
}
