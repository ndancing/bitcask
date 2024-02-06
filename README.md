# bitcask

A simple Bitcask implementation in Java

**Bitcask**

A Log-Structured Hash Table for Fast Key/Value Data

**What does Bitcask offer?**

1. Low latency per item read or written
2. High throughput, especially when writing an incoming stream of random items
3. Crash friendliness, both in terms of fast recovery and not losing data
4. Ease of backup and restore

**Bitcask paper**

https://riak.com/assets/bitcask-intro.pdf

### Server
**1. Storage config**

| Properties      | Description             | Default Value |
|-----------------|-------------------------|---------------|
| storageDir      | Data stored directory   | N/A           |
| cacheEnabled    | Enable/disable cache    | true          |
| cacheSize       | Cache size (if enabled) | 10000         |
| fileSizeLimit   | Data file size limit    | 1MB           |
| mergePeriodMils | Data file merge period  | 5 mins        |

**2. Start a Bitcask Server**

Application.java
```Java
int port = Integer.valueOf(System.getProperty("port"));
String storageDir = System.getProperty("storageDir");
BitcaskStorageConfig storageConfig = BitcaskStorageConfig.builder()
    .storageDir(storageDir)
    .cacheEnabled(true).cacheSize(10000)
    .fileSizeLimit(1024000).mergePeriodMils(300000)
    .build();

BitcaskServer server = new BitcaskServer(port, storageConfig);
server.start();
```

Execution:
```shell
mvn clean install
mvn exec:java -Dexec.mainClass=com.github.example.kv.server.Application -Dport=6868 -DstorageDir=/data/bcask
```
**3. Command Line**

After start a Bitcask server, using the console to execute these commands:

- Store key/value pair to storage
```shell
SET example_key "example value"
```

- Get value of a key from storage
```shell
GET example_key
```

- Remove a key/value pair
```shell
RMV example_key
```
**Notes:**

_Key must not contain any whitespace and value must be surrounded by double quotes_

### Java Client Usage

**1. Connect to a Bitcask server**
```java
BitcaskClient client = Bitcask.builder()
    .host("127.0.0.1")
    .port(6868)
    .timeOutMils(3000)
    .build();
```

**2. API Usage**

Bitcask key interface
```java
BKey<String> bcaskKey = client.getKey("example_key");
```
Throws **BitcaskException** if key is empty or contains any whitespace

- Set a key/value pair
```java
bcaskKey.setValue("example value");
```
Return **true** if set key/value successfully, otherwise return **false**

- Get value of a key from storage
```java
bcaskKey.getValue();
```
Return value of key or throws **BitcaskNotExistedKeyException** if key is not existed

- Remove a key
```java
bcaskKey.remove();
```
Return true if remove key successfully, otherwise return false or throws **BitcaskNotExistedKeyException** if key is not existed

### About Storage

**How does the data file merging work?**

Steps

1. Get all stored file exclude currently active file -> Mergeable files
2. Scan from closest to furthest mergeable files -> Build a key-value mapping from each one. Using a metadata container to store last key-metadata mapping
3. Create a compacted file to merge all processed files . Give it a name like to the closest merged file append with a suffix (to avoid duplicated file name)
4. Write all last mapping data to the compact file. In this step need to check and remove deleted key (tombstone value)
5. Update the merged metadata with the closest merged file path
6. Delete all merged files then rename the compact file to closest merged file
7. Update the merged mapping metadata to storage

**Storage Benchmark**

- Environment: MacBook Pro (16-inch, 2021), Apple M1 Pro, 16GB, SSD 512GB
- No buffered file output
- No cache enabled

| Method | File Size | Number of keys | Key size (bytes) | Value size (bytes) | Time (secs) | Memory usage (MB) | 
|--------|-----------|----------------|------------------|--------------------|-------------|-------------------|
| Put    | 10MB      | 1 000 000      | 8                | 128                | 7,67        | 542               |
| Get    | 10MB      | 1 000 000      | 8                | 128                | 1,42        |                   |
| Put    | 10MB      | 1 000 000      | 16               | 256                | 8,17        | 759               |
| Get    | 10MB      | 1 000 000      | 16               | 256                | 1,58        |                   |
| Put    | 100MB     | 5 000 000      | 8                | 128                | 39,94       | 2573              |
| Get    | 100MB     | 5 000 000      | 8                | 128                | 7,60        |                   |
