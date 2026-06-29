import { IKeyedDB, Identifiable, Comparable, PaginationMode } from "./Types";
export default class KeyedDB<T, K> implements IKeyedDB<T, K> {
    private array;
    private dict;
    private key;
    private idGetter;
    /**
     * @param key Return the unique key used to sort items
     * @param id The unique ID for the items
     */
    constructor(key: Comparable<T, K>, id?: Identifiable<T>);
    get length(): number;
    get first(): T;
    get last(): T;
    toJSON(): T[];
    /**
     * Inserts items into the DB in klogN time.
     * Where k is the number of items being inserted.
     * @param values
     */
    insert(...values: T[]): void;
    /**
     * Upserts items into the DB in 2klogN time.
     * Where k is the number of items being inserted.
     *
     * If a duplicate is found, it is deleted first and then the new one is inserted
     * @param values
     * @returns list of updated values
     */
    upsert(...values: T[]): T[];
    /**
     * Inserts items only if they are not present in the DB
     * @param values
     * @returns list of all the inserted values
     */
    insertIfAbsent(...values: T[]): T[];
    /**
     * Deletes an item indexed by the ID
     * @param id
     * @param assertPresent
     */
    deleteById(id: string, assertPresent?: boolean): T;
    delete(value: T): T;
    slice(start?: number, end?: number): KeyedDB<T, K>;
    /** Clears the DB */
    clear(): void;
    get(id: string): T;
    all(): T[];
    /**
     * Updates a value specified by the ID
     * and adjusts its position in the DB after an update if required
     * @param id
     * @param update
     */
    update(id: string, update: (value: T) => void): 1 | 2;
    /**
     * @deprecated see `update`
     */
    updateKey(value: T, update: (value: T) => void): 1 | 2;
    filter(predicate: (value: T, index: number) => boolean): KeyedDB<T, K>;
    /**
     * Get the values of the data in a paginated manner
     * @param value the value itself beyond which the content is to be retreived
     * @param limit max number of items to retreive
     * @param predicate optional filter
     * @param mode whether to get the content `before` the cursor or `after` the cursor; default=`after`
     */
    paginatedByValue(value: T | null, limit: number, predicate?: (value: T, index: number) => boolean, mode?: PaginationMode): T[];
    /**
     * Get the values of the data in a paginated manner
     * @param value the cursor beyond which the content is to be retreived
     * @param limit max number of items to retreive
     * @param predicate optional filter
     * @param mode whether to get the content `before` the cursor or `after` the cursor; default=`after`
     */
    paginated(cursor: K | null, limit: number, predicate?: (value: T, index: number) => boolean, mode?: PaginationMode): T[];
    private _insertSingle;
    private filtered;
    private firstIndex;
}
