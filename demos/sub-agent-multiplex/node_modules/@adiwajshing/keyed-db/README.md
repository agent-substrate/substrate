# Keyed DB

A light-weight node library to manage a sorted & indexed collection with pagination support. 
All done using Binary Search. Based off my swift code for [Queenfisher](https://github.com/adiwajshing/Queenfisher)

## Install

`npm i github:adiwajshing/keyed-db`

## Running Tests

`npm test`

## Functions

``` ts

db = new KeyedDB<T> (t => t.uniqueNumberKeyProperty, t => t.optionalUniqueIDProperty)
// compare with a custom function
db = new KeyedDB<T> ({  
    key: t => t.someProperty,
    compare: (t1, t2) => someComputation(t1, t2) // return -1 if t1 < t2, 0 if t1=t2 & 1 if t1 > t2
}, t => t.optionalUniqueIDProperty)

db.insert (value) // insert value in DB
db.upsert (value) // upserts value
db.insertIfAbsent (value) // only inserts if not already present in DB
db.delete (value) // delete value
db.deleteById (value.optionalUniqueIDProperty) // delete value by referencing the ID
// update the key of a value, 
// will automatically place object after key change
db.updateKey (value, value => value.uniqueKeyProperty = newValue) 
db.paginated (someCursor, 20) // get X results after the given cursor (null for the first X results)

```

## Usage

``` ts
import KeyedDB from '@adiwajshing/keyed-db'

// Let's use the db to sort & maintain a list of chats
// Chats must be accessed quickly via the chatID (the person you're chatting with)
// Chats must be sorted by recency
type Chat = {
    timestamp: Date
    chatID: string
}

// first argument -- sorting property, second argument -- ID property
const db = new KeyedDB<Chat>(value => value.timestamp.getTime()*-1, value => value.chatID)

for (let i = 0; i < 1000;i++) {
    // insert data
    db.insert (
        {
            timestamp: new Date( new Date().getTime() - Math.random()*10000 ), 
            chatID: `person ${i}`
        }
    )
}
console.log (db.all()) // return internal sorted array
console.log (db.paginated(null, 20)) // return first 20 chats
console.log (db.paginated(null, 20, null, 'before')) // return last 20 chats
console.log (db.paginated(null, 20, chat => chat.chatID.includes('something'))) // return first 20 chats where the chatID contains 'something'

const someDate = new Date().getTime()
const cursorPaginated = db.paginated(someDate, 20)
console.log (cursorPaginated) // return 20 chats after the specified date

db.delete (cursorPaginated[0]) // delete paginated chats 

// update chat timestamp
db.updateKey(cursorPaginated[1], value => value.timestamp = new Date().getTime()) 

```

## Time Complexity

| Operation      | Time Complexity |
|----------------|-----------------|
| db.insert()    | O(logN)         |
| db.delete()    | O(logN)         |
| db.get()       | O(1)            |
| db.updateKey() | O(logN)         |