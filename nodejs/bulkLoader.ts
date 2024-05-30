import {TypeDB} from "typedb-driver/TypeDB";
import {SessionType, TypeDBSession} from "typedb-driver/api/connection/TypeDBSession";
import {TransactionType, TypeDBTransaction} from "typedb-driver/api/connection/TypeDBTransaction";
import * as fs from "fs";
import * as readline from "readline";
import {TypeDBCredential, TypeDBDriver} from "typedb-driver";
import * as os from "os";

class QueryIterator implements AsyncIterable<string> {
    private filepaths: Array<string>;
    constructor(filepaths: Array<string>) {
        this.filepaths = filepaths;
    }

    async *[Symbol.asyncIterator](): AsyncIterableIterator<string> {

        for (let filepath of this.filepaths) {
            let file = readline.createInterface(fs.createReadStream(filepath));

            for await (let query of file) {
                yield query;
            }
        }
    }
}

class BatchIterator implements AsyncIterable<Array<string>> {
    private filepaths: Array<string>;
    private batchSize: number;

    constructor(filepaths: Array<string>, batchSize: number) {
        this.filepaths = filepaths;
        this.batchSize = batchSize;
    }

    async *[Symbol.asyncIterator](): AsyncIterator<Array<string>> {
        let queryIterator = new QueryIterator(this.filepaths);
        let nextBatch: Array<string> = [];

        for await (let query of queryIterator) {
            nextBatch.push(query);

            if (nextBatch.length >= this.batchSize) {
                yield nextBatch;
                nextBatch = [];
            }
        }

        if (nextBatch.length > 0) { yield nextBatch }
    }
}

class PromiseQueue<T> {
    public size: number;
    public promises: Array<Promise<{index: number; result: T}>>;
    constructor(size: number) {
        this.size = size;
        this.promises = [];
    }

    async put(value: Promise<T>): Promise<void | T> {
        if (this.promises.length < this.size) {
            let index = this.promises.length;
            this.promises.push(value.then(result => ({index: index, result})));
            return;
        } else {
            let {index, result} = await Promise.race(this.promises)
            this.promises[index] = value.then(result => ({index: index, result}));
            return result;
        }
    }

    async join(): Promise<Array<T>> {
        let results: Array<T> = [];
        let outputs = await Promise.all(this.promises)

        for (let output of outputs) {
            results.push(output.result)
        }

        return results;
    }
}

async function loadBatch(batch: Array<string>, session: TypeDBSession): Promise<void> {
    let transaction: TypeDBTransaction;

    try {
        transaction = await session.transaction(TransactionType.WRITE);

        for (let query of batch) {
            transaction.query.insert(query)
        }

        await transaction.commit();
    } finally {
        if (transaction.isOpen()) {
            await transaction.close();
        }
    }
}

async function loadDataAsync(addresses: Array<string>, username: string, password: string): Promise<void> {
    const database: string = "bulk-load-test";
    const dataFiles: Array<string> = ["../dataset/entities.tql", "../dataset/relations.tql"];
    const batchSize: number = 100;

    let credential: TypeDBCredential = new TypeDBCredential(username, password);
    let driver: TypeDBDriver;

    try {
        driver = await TypeDB.cloudDriver(addresses, credential);
        let session: TypeDBSession;

        try {
            session = await driver.session(database, SessionType.DATA);

            for (let dataFile of dataFiles) {
                let queueSize = os.cpus().length;
                let promiseQueue = new PromiseQueue(queueSize);
                let batchIterator = new BatchIterator([dataFile], batchSize);

                for await (let batch of batchIterator) {
                    await promiseQueue.put(loadBatch(batch, session));
                }

                await promiseQueue.join();
            }
        } finally { await session?.close() }
    } finally { await driver?.close() }
}

async function main() {
    const addresses = ["localhost:1729"];
    const username = "null";
    const password = "null";
    await loadDataAsync(addresses, username, password);
}

main();
