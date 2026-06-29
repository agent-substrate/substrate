"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const BinarySearch_1 = __importDefault(require("./BinarySearch"));
class KeyedDB {
    /**
     * @param key Return the unique key used to sort items
     * @param id The unique ID for the items
     */
    constructor(key, id) {
        this.key = key;
        this.idGetter = id || (v => this.key.key(v).toString());
        this.dict = {};
        this.array = [];
    }
    get length() {
        return this.array.length;
    }
    get first() {
        return this.array[0];
    }
    get last() {
        return this.array[this.array.length - 1];
    }
    toJSON() {
        return this.array;
    }
    /**
     * Inserts items into the DB in klogN time.
     * Where k is the number of items being inserted.
     * @param values
     */
    insert(...values) {
        values.forEach(v => this._insertSingle(v));
    }
    /**
     * Upserts items into the DB in 2klogN time.
     * Where k is the number of items being inserted.
     *
     * If a duplicate is found, it is deleted first and then the new one is inserted
     * @param values
     * @returns list of updated values
     */
    upsert(...values) {
        const updates = [];
        values.forEach(v => {
            if (!v)
                return;
            const deleted = this.deleteById(this.idGetter(v), false);
            this._insertSingle(v);
            // add to updates
            deleted && updates.push(v);
        });
        return updates;
    }
    /**
     * Inserts items only if they are not present in the DB
     * @param values
     * @returns list of all the inserted values
     */
    insertIfAbsent(...values) {
        const insertions = [];
        values.forEach(v => {
            if (!v)
                return;
            // if ID is present
            const presentValue = this.get(this.idGetter(v));
            if (presentValue)
                return;
            // if key is present
            const presentKey = this.firstIndex(v);
            if (this.array[presentKey] && this.key.key(this.array[presentKey]) === this.key.key(v))
                return;
            this.insert(v);
            insertions.push(v);
        });
        return insertions;
    }
    /**
     * Deletes an item indexed by the ID
     * @param id
     * @param assertPresent
     */
    deleteById(id, assertPresent = true) {
        const value = this.get(id);
        if (!value) {
            if (assertPresent)
                throw new Error(`Value not found`);
            else
                return;
        }
        return this.delete(value);
    }
    delete(value) {
        const index = this.firstIndex(value);
        if (index < 0 || index >= this.array.length || this.key.key(value) !== this.key.key(this.array[index])) {
            return null;
        }
        delete this.dict[this.idGetter(value)];
        return this.array.splice(index, 1)[0];
    }
    slice(start, end) {
        const db = new KeyedDB(this.key, this.idGetter);
        db.array = this.array.slice(start, end);
        db.array.forEach(item => db.dict[this.idGetter(item)] = item);
        return db;
    }
    /** Clears the DB */
    clear() {
        this.array = [];
        this.dict = {};
    }
    get(id) {
        return this.dict[id];
    }
    all() {
        return this.array;
    }
    /**
     * Updates a value specified by the ID
     * and adjusts its position in the DB after an update if required
     * @param id
     * @param update
     */
    update(id, update) {
        const value = this.get(id);
        if (value) {
            const idx = this.firstIndex(value);
            if (idx >= 0 && idx < this.array.length && this.idGetter(this.array[idx]) === id) {
                const oldKey = this.key.key(value);
                update(value);
                const newKey = this.key.key(value);
                if (newKey !== oldKey) {
                    delete this.dict[id];
                    this.array.splice(idx, 1);
                    this._insertSingle(value);
                    return 2;
                }
                return 1;
            }
        }
    }
    /**
     * @deprecated see `update`
     */
    updateKey(value, update) {
        return this.update(this.idGetter(value), update);
    }
    filter(predicate) {
        const db = new KeyedDB(this.key, this.idGetter);
        db.array = this.array.filter((value, index) => {
            if (predicate(value, index)) {
                db.dict[this.idGetter(value)] = value;
                return true;
            }
        });
        return db;
    }
    /**
     * Get the values of the data in a paginated manner
     * @param value the value itself beyond which the content is to be retreived
     * @param limit max number of items to retreive
     * @param predicate optional filter
     * @param mode whether to get the content `before` the cursor or `after` the cursor; default=`after`
     */
    paginatedByValue(value, limit, predicate, mode = 'after') {
        return this.paginated(value && this.key.key(value), limit, predicate, mode);
    }
    /**
     * Get the values of the data in a paginated manner
     * @param value the cursor beyond which the content is to be retreived
     * @param limit max number of items to retreive
     * @param predicate optional filter
     * @param mode whether to get the content `before` the cursor or `after` the cursor; default=`after`
     */
    paginated(cursor, limit, predicate, mode = 'after') {
        let index = mode === 'after' ? 0 : this.array.length;
        if (cursor !== null && typeof cursor !== 'undefined') {
            index = BinarySearch_1.default(this.array, v => this.key.compare(cursor, this.key.key(v)));
            if (index < 0)
                index = 0;
            if (this.key.key(this.array[index]) === cursor)
                index += (mode === 'after' ? 1 : 0);
        }
        return this.filtered(index, limit, mode, predicate);
    }
    _insertSingle(value) {
        if (!value)
            throw new Error('falsey value');
        const valueID = this.idGetter(value);
        if (this.get(valueID)) {
            throw new Error('duplicate ID being inserted: ' + valueID);
        }
        if (this.array.length > 0) {
            const index = this.firstIndex(value);
            if (index >= this.array.length)
                this.array.push(value);
            else if (index < 0)
                this.array.unshift(value);
            else if (this.key.key(value) !== this.key.key(this.array[index]))
                this.array.splice(index, 0, value);
            else
                throw new Error(`duplicate key: ${this.key.key(value)}, of inserting: ${valueID}, present: ${this.idGetter(this.array[index])}`);
        }
        else {
            this.array.push(value);
        }
        this.dict[valueID] = value;
    }
    filtered(start, count, mode, predicate) {
        let arr;
        if (mode === 'after') {
            if (predicate) {
                arr = [];
                for (let item of this.array.slice(start)) {
                    predicate(item, start + arr.length) && arr.push(item);
                    if (arr.length >= count)
                        break;
                }
            }
            else
                arr = this.array.slice(start, start + count);
        }
        else if (mode === 'before') {
            if (predicate) {
                arr = [];
                for (let i = start - 1; i >= 0; i--) {
                    let item = this.array[i];
                    predicate(item, start + arr.length) && arr.unshift(item);
                    if (arr.length >= count)
                        break;
                }
            }
            else
                arr = this.array.slice(Math.max(start - count, 0), start);
        }
        return arr;
    }
    firstIndex(value) {
        const valueKey = this.key.key(value);
        return BinarySearch_1.default(this.array, v => this.key.compare(valueKey, this.key.key(v)));
    }
}
exports.default = KeyedDB;
