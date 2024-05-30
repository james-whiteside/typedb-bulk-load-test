package org.example;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class QueryIterator implements Iterator<String> {
    public final List<String> filepaths;
    private Iterator<String> lineIterator;
    private boolean finished;

    public QueryIterator(ArrayList<String> filepaths) throws FileNotFoundException {
        this.filepaths = filepaths;
        finished = false;
        prepNextIterator();
    }

    private static InputStream inputStream(String filepath) throws FileNotFoundException {
        try {
            return new BufferedInputStream(new FileInputStream(filepath), 128_000);
        } catch (IOException e) {
            throw new FileNotFoundException();
        }
    }

    private void prepNextIterator() throws FileNotFoundException {
        if (this.filepaths.isEmpty()) {
            finished = true;
        } else {
            InputStream inputStream = inputStream(filepaths.remove(0));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, UTF_8));
            lineIterator = bufferedReader.lines().iterator();
            if (!lineIterator.hasNext()) prepNextIterator();
        }
    }

    @Override
    public boolean hasNext() {
        return !finished;
    }

    @Override
    public String next() {
        String nextItem = lineIterator.next();

        if (!lineIterator.hasNext()) {
            try {
                this.prepNextIterator();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        return nextItem;
    }
}
