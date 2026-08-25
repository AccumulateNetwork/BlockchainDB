# Basic Key-Value Operations

This example demonstrates how to perform basic key-value operations
with BlockchainDB.

Keys are 32 bytes and values are arbitrary byte slices.  A database has
two layers, and which one you write to is the main decision the API
asks you to make:

- **Perm** is immutable.  A key written to Perm keeps its value
  forever; writing a *different* value for a key already in Perm is an
  error.  This is where content-addressed data goes -- transactions,
  blocks, anything keyed by its own hash.
- **Dyna** is mutable.  Keys can be rewritten freely, and the space
  the old values took is reclaimed by compaction.  This is where
  state goes.

`Get` checks Dyna first, then Perm, so a key moved from Perm to Dyna
reads back as its new value.

## Creating a New Database

```go
package main

import (
	"fmt"
	"os"

	blockchainDB "github.com/AccumulateNetwork/BlockchainDB/database"
)

func main() {
	// Create a directory for the database
	dbDir := "./mydb"
	os.MkdirAll(dbDir, os.ModePerm)

	// Create a new sharded store.  sealLimit is the point at which a
	// layer seals its live tail into an immutable segment: bigger
	// means fewer, larger segments and a longer replay on open.
	kvs, err := blockchainDB.NewKVShard(dbDir, 100_000)
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		return
	}
	defer kvs.Close()

	fmt.Println("Database created successfully")

	performOperations(kvs)
}

func performOperations(kvs *blockchainDB.KVShard) {
	key1 := createKey("key1")
	key2 := createKey("key2")

	// Immutable data goes in the Perm layer
	if err := kvs.PutPerm(key1, []byte("Hello, BlockchainDB!")); err != nil {
		fmt.Printf("Error storing value: %v\n", err)
		return
	}

	// Mutable state goes in the Dyna layer
	if err := kvs.PutDyna(key2, []byte("This is another value")); err != nil {
		fmt.Printf("Error storing value: %v\n", err)
		return
	}

	fmt.Println("Values stored successfully")

	// Get resolves the layers: Dyna first, then Perm
	value1, err := kvs.Get(key1)
	if err != nil {
		fmt.Printf("Error retrieving value: %v\n", err)
		return
	}
	fmt.Printf("Value for key1: %s\n", value1)

	value2, err := kvs.Get(key2)
	if err != nil {
		fmt.Printf("Error retrieving value: %v\n", err)
		return
	}
	fmt.Printf("Value for key2: %s\n", value2)

	// Dyna values can be rewritten
	if err := kvs.PutDyna(key2, []byte("Updated value")); err != nil {
		fmt.Printf("Error updating value: %v\n", err)
		return
	}

	updated, err := kvs.Get(key2)
	if err != nil {
		fmt.Printf("Error retrieving updated value: %v\n", err)
		return
	}
	fmt.Printf("Updated value for key2: %s\n", updated)

	// Rewriting a Perm key with a different value is rejected.  To
	// change a value that is already in Perm, write the new value to
	// Dyna -- Get resolves to it.
	if err := kvs.PutPerm(key1, []byte("A different value")); err != nil {
		fmt.Printf("Expected error rewriting an immutable key: %v\n", err)
	}
}

// Helper function to create a 32-byte key from a string
func createKey(input string) [32]byte {
	var key [32]byte
	copy(key[:], input)
	return key
}
```

## Opening an Existing Database

```go
package main

import (
	"fmt"

	blockchainDB "github.com/AccumulateNetwork/BlockchainDB/database"
)

func main() {
	dbDir := "./mydb"
	kvs, err := blockchainDB.OpenKVShard(dbDir)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		return
	}
	defer kvs.Close()

	fmt.Println("Database opened successfully")

	key := createKey("key1")
	value, err := kvs.Get(key)
	if err != nil {
		fmt.Printf("Error retrieving value: %v\n", err)
		return
	}
	fmt.Printf("Value for key1: %s\n", value)
}

// Helper function to create a 32-byte key from a string
func createKey(input string) [32]byte {
	var key [32]byte
	copy(key[:], input)
	return key
}
```

`Close` is the durability point: after it returns, everything written
before it is on disk and survives a crash.  See
[Crash durability](../design/durability.md) for the exact contract.

## Working with Binary Data

BlockchainDB works well with binary data, which is common in
blockchain applications.  A content hash is the natural Perm key:

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	blockchainDB "github.com/AccumulateNetwork/BlockchainDB/database"
)

func main() {
	dbDir := "./bindb"
	os.MkdirAll(dbDir, os.ModePerm)

	kvs, err := blockchainDB.NewKVShard(dbDir, 100_000)
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		return
	}
	defer kvs.Close()

	// A content hash keys the data it hashes, so the value can never
	// legitimately change -- exactly what the Perm layer assumes
	data := []byte("some transaction data")
	hash := sha256.Sum256(data)

	binaryData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	if err := kvs.PutPerm(hash, binaryData); err != nil {
		fmt.Printf("Error storing binary data: %v\n", err)
		return
	}

	fmt.Printf("Stored binary data with key: %s\n", hex.EncodeToString(hash[:]))

	retrieved, err := kvs.Get(hash)
	if err != nil {
		fmt.Printf("Error retrieving binary data: %v\n", err)
		return
	}

	fmt.Printf("Retrieved binary data: %v\n", retrieved)
	fmt.Printf("Hex representation: %s\n", hex.EncodeToString(retrieved))
}
```

## Error Handling

Proper error handling is important when working with BlockchainDB:

```go
package main

import (
	"fmt"
	"os"

	blockchainDB "github.com/AccumulateNetwork/BlockchainDB/database"
)

func main() {
	dbDir := "./errdb"
	os.MkdirAll(dbDir, os.ModePerm)

	kvs, err := blockchainDB.NewKVShard(dbDir, 100_000)
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		return
	}
	defer kvs.Close()

	var key [32]byte
	copy(key[:], "nonexistent-key")

	// A missing key is an error, not an empty value
	value, err := kvs.Get(key)
	if err != nil {
		fmt.Printf("Expected error when retrieving non-existent key: %v\n", err)
	} else {
		fmt.Printf("Retrieved value: %s\n", value)
	}

	if err := kvs.PutDyna(key, []byte("Now the key exists")); err != nil {
		fmt.Printf("Error storing value: %v\n", err)
		return
	}

	value, err = kvs.Get(key)
	if err != nil {
		fmt.Printf("Unexpected error: %v\n", err)
	} else {
		fmt.Printf("Retrieved value: %s\n", value)
	}
}
```
