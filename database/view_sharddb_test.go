package blockchainDB

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestView(t *testing.T) {

	NumKeys := 1000            // Number of keys used in the test
	Timeout := time.Second * 1 // The timeout period

	Directory, rm := MakeDir()
	defer rm()
	sdbv, err := NewShardDBViews(Directory, Timeout, 1, 256, 1)
	assert.NoError(t, err, "failed to open ShardDBViews")

	// Collect NumKeys number of key/values, and populate the DB
	fr := NewFastRandom([]byte{1, 2, 3})
	keys := make(map[[32]byte][]byte) // A set of keys and their original values
	for i := 0; i < NumKeys; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(10, 20)
		keys[key] = value
		sdbv.Put(key, value)
	}

	// Make all the key/values collected are in fact in the DB
	for k, v := range keys {
		value := sdbv.Get(k)
		assert.Equal(t, v, value, "Failed to get our value back")
	}

	// Create a series of views, and make sure they all see that the key
	// value pairs are unchanged from the point the view was created.
	// After we are done, we make sure all the views timeout and go away.
	for i := 0; i < 10; i++ {
		fmt.Printf("%4d AV %4d \n", i, len(sdbv.ActiveViews))
		// collect the current state of keys
		for k := range keys {
			keys[k] = sdbv.Get(k)
		}

		// Modify the some of the keys, and collect the updated values
		// This should not impact the view
		updated := make(map[[32]byte][]byte) // make a list of updated keys
		view := sdbv.NewView()               // Create a view
		for k := range keys {                //                Modify 30 percent or so of the keys
			if fr.UintN(10) < 30 {
				fr.RandBuff(10, 20)
				value := fr.RandBuff(10, 20)
				updated[k] = value
				sdbv.Put(k, value)
			}
		}

		// check that none of the values changed in the view even if the
		// database have been updated.
		for k, v := range keys {
			value, err := sdbv.ViewGet(view, k)
			assert.NoError(t, err, "view failed")
			assert.Equal(t, v, value)
		}

		// check that none of the values changed in the view
		for k, v := range keys {
			value, err := sdbv.ViewGet(view, k)
			assert.NoError(t, err, "view failed")
			assert.Equal(t, v, value)
		}

		// check that the DB state is all still good
		for k, v := range updated {
			value := sdbv.Get(k)
			assert.Equal(t, v, value)
		}
		for k, v := range keys {
			value := sdbv.Get(k)
			if value2, ok := updated[k]; ok {
				assert.Equal(t, value, value2, "updated value lost")
			} else {
				assert.Equal(t, value, v, "original value wrong")
			}
		}

		time.Sleep(Timeout / 5) // sleep for a bit to allow some views to timeout
	}

	time.Sleep(Timeout * 10 / 11)
	sdbv.IsViewActive()
	assert.False(t, sdbv.IsViewActive(), "no view should be active")
	assert.True(t, len(sdbv.ActiveViews) == 0, "nothing should be in ActiveViews")

}
