"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Binary search
 * @param array the array to search in
 * @param predicate return a value of < 0, if the item you're looking for should come before, 0 if it is the item you're looking for
 */
function binarySearch(array, predicate) {
    let low = 0;
    let high = array.length;
    // base cases to allow entering a sorted collection in O(N)
    if (array.length === 0)
        return low;
    if (predicate(array[low]) < 0)
        return low - 1;
    else if (predicate(array[low]) === 0)
        return low;
    const maxPred = predicate(array[high - 1]);
    if (maxPred > 0)
        return high;
    else if (maxPred === 0)
        return high - 1;
    while (low !== high) {
        const mid = low + Math.floor((high - low) / 2);
        const pred = predicate(array[mid]);
        if (pred < 0)
            high = mid;
        else if (pred > 0)
            low = mid + 1;
        else
            return mid;
    }
    return low;
}
exports.default = binarySearch;
