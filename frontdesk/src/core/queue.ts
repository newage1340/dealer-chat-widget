/**
 * An unbounded async queue: producers push, one consumer iterates until it is closed.
 */
export class AsyncQueue<T> implements AsyncIterable<T> {
    #items: T[] = [];
    #waiting: ((result: IteratorResult<T>) => void) | null = null;
    #closed = false;

    push(item: T): void {
        if (this.#closed) {
            return;
        }
        if (this.#waiting !== null) {
            const resolve = this.#waiting;
            this.#waiting = null;
            resolve({ value: item, done: false });
        } else {
            this.#items.push(item);
        }
    }

    close(): void {
        this.#closed = true;
        if (this.#waiting !== null) {
            const resolve = this.#waiting;
            this.#waiting = null;
            resolve({ value: undefined, done: true });
        }
    }

    get closed(): boolean {
        return this.#closed;
    }

    [Symbol.asyncIterator](): AsyncIterator<T> {
        return {
            next: () => {
                if (this.#items.length > 0) {
                    return Promise.resolve({ value: this.#items.shift()!, done: false });
                }
                if (this.#closed) {
                    return Promise.resolve({ value: undefined, done: true });
                }
                return new Promise(resolve => {
                    this.#waiting = resolve;
                });
            },
        };
    }
}
