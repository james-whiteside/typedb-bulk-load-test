package org.example;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BatchIterator implements Iterator<List<String>> {
    public int batchSize;
    private final Iterator<String> queries;

    public BatchIterator(ArrayList<String> filepaths, int batchSize) throws FileNotFoundException {
        this.batchSize = batchSize;
        this.queries = new QueryIterator(filepaths);
    }

    @Override
    public boolean hasNext() { return queries.hasNext(); }

    @Override
    public List<String> next() {
        if (!this.hasNext()) throw new NoSuchElementException();
        List<String> batch = new ArrayList<>();

        while (queries.hasNext() && batch.size() < batchSize) {
            batch.add(queries.next());
        }

        return batch;
    }
}
