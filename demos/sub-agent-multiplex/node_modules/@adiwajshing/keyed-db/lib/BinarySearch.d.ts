/**
 * Binary search
 * @param array the array to search in
 * @param predicate return a value of < 0, if the item you're looking for should come before, 0 if it is the item you're looking for
 */
export default function binarySearch<T>(array: T[], predicate: (t: T) => number): number;
