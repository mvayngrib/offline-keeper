# offline-keeper

[![NPM](https://nodei.co/npm/bindall.png)](https://nodei.co/npm/offline-keeper/)

# Usage

See tests, the API is very basic

## putOne(String, Buffer)

Returns a Q.Promise that resolves when the value is written to disk

## getOne(String)

Returns a Q.Promise that resolves to a Buffer

## removeOne(String)

Returns a Q.Promise that resolves when the corresponding value is erased from the disk

## getMany([String, ...])

Returns a Q.Promise that resolves to an array with values (Buffer) and/or nulls, depending on if values exist on disk or not

## clear

Deletes all stored values

## close

Finish pending writes and die
