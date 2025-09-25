package blockchainDB

import (
	"fmt"
	"time"
)

// View implementation
// Simplifying assumptions of this implementation include:
//   - Most DB accesses will not be done within the context of a view
//   - Views will collect until all views timeout, then they can all be cleared
//   - Using a view can reasonably timeout in 30 seconds.  Every access
//     can reset the timeout, so a view can stay open as long as it is
//     actively used
//   - Anytime the DB is accessed, we can check to see if we need to
//     clear the views and flush data to disk
//   - Currently rebuilding the DB after an unclean close of the DB is
//     not supported

// View Struct
// Views allow one to grab a point in time in the database, and query values that were
// in the DB at that time (writes after creating a view do not impact the view)
type View struct {
	ID         int                 // ID of a view
	KeyValues  map[[32]byte][]byte // key value pairs
	LastAccess time.Time           // time that the view was created
	Closed     bool                // true if the View is closed
	KVView     *KVView             // The KV database with views
}

func (v *View) Get(key [32]byte) (value []byte, err error) {
	return v.KVView.ViewGet(v, key)
}

// KVView
// A wrapper around a sharded DB that implements Views.  Views are created in a stack.
// More recent views lookup values in their cache, and then in the views that come
// before.  Later views are ignored.
type KVView struct {
	DB          *KVShard      // The underlying DB
	ViewID      int           // The next ViewID
	ActiveViews []*View       // List of all active Views, newest first
	Map         map[int]View  // Fast lookup of a view
	Timeout     time.Duration // How long before views timeout; every access resets timeout
	OffsetCnt   uint64        // KeyOffset number for the key files
	KeyLimit    uint64        // KeyLimit sets when to move keys to History
}

func NewShardDBViews(
	Directory string,
	Timeout time.Duration,
	Partition, ShardCnt,
	BufferCnt,
	OffsetCnt, KeyLimit uint64,
	MaxCachedBlocks int) (sdbV *KVView, err error) {

	sdbV = new(KVView)
	sdbV.Timeout = Timeout
	sdbV.OffsetCnt = OffsetCnt
	sdbV.KeyLimit = KeyLimit
	if sdbV.DB, err = NewKVShard(Directory, sdbV.OffsetCnt, sdbV.KeyLimit, MaxCachedBlocks); err == nil {
		return sdbV, nil
	}
	return nil, err
}

func OpenShardDBViews(
	Directory string,
	Timeout time.Duration,
	Partition,
	BufferCnt int) (sdbV *KVView, err error) {

	sdbV = new(KVView)
	sdbV.Timeout = Timeout
	if sdbV.DB, err = OpenKVShard(Directory); err == nil {
		return sdbV, nil
	}
	return nil, err
}

// Flush writes any cached updates to disk
func (s *KVView) Flush() error {
	// Flush any cached updates to disk
	if len(s.ActiveViews) > 0 && s.ActiveViews[0] != nil {
		count := 0
		// Write all cached key-values to the database
		for key, value := range s.ActiveViews[0].KeyValues {
			if err := s.DB.Put(key, value); err != nil {
				return fmt.Errorf("flush failed after %d puts: %v", count, err)
			}
			count++
		}
		// Clear the cache after flushing
		s.ActiveViews[0].KeyValues = make(map[[32]byte][]byte)
	}
	return nil
}

func (s *KVView) Close() error {
	// Collapse all views - write all cached data to the database
	// When views are active, updates are cached and must be persisted
	if len(s.ActiveViews) > 0 {
		// The cache at index 0 has all the updates that need to be persisted
		if s.ActiveViews[0] != nil {
			for key, value := range s.ActiveViews[0].KeyValues {
				// Write each cached update to the actual database
				if err := s.DB.Put(key, value); err != nil {
					// Log error but continue to flush remaining data
					// In production, this should be logged
				}
			}
		}
	}

	// Clear all views
	s.ActiveViews = nil
	s.Map = nil

	// Now close the underlying database, which will persist everything to disk
	return s.DB.Close()
}

// Active Views
// Returns true if a valid active view exists.  If old views
// exist, but none are active, the active views are tossed.
func (s *KVView) IsViewActive() bool {
	// The first entry in the ActiveViews is the View Cache
	// ActiveViews with a length less than two means no active views
	if len(s.ActiveViews) < 2 {
		return false
	}

	s.GetViewIndex(s.ActiveViews[1]) // This will clear ActiveViews if none are valid
	return len(s.ActiveViews) > 1    // Need more than just the cache (index 0)
}

func (s *KVView) Put(key [32]byte, value []byte) error {

	// If not view is active, then write to the DB
	if !s.IsViewActive() {
		return s.DB.Put(key, value)
	}

	// Only put the key/value in the cache (index 0), not in user views
	s.ActiveViews[0].KeyValues[key] = value
	return nil
}

func (s *KVView) Get(key [32]byte) (value []byte, err error) {

	// If no view is active, just get the DB value
	if !s.IsViewActive() {
		return s.DB.Get(key)
	}

	// If a view is active, we need to check the view cache first
	value, ok := s.ActiveViews[0].KeyValues[key]
	if ok {
		return value, nil
	}

	return s.DB.Get(key) // Nothing in the view cache? Pull from the DB
}

func (s *KVView) NewView() *View {
	// If no view is active, we have to cache DB updates so
	// create a "DB Update View" at s.ActiveViews[0]
	if !s.IsViewActive() {
		view := new(View)
		view.ID = 0
		view.KeyValues = make(map[[32]byte][]byte)
		s.ActiveViews = append(s.ActiveViews, view)
	}

	// Create a view and added it to the ActiveViews slice. Add
	// the newest views to the end of the list.
	view := new(View)
	s.ViewID++
	view.KVView = s
	view.ID = s.ViewID
	view.LastAccess = time.Now()
	view.KeyValues = make(map[[32]byte][]byte)

	// Copy the current cache state into the new view so it sees the state at creation time
	// This ensures the view sees all updates that happened before it was created
	if len(s.ActiveViews) > 0 && s.ActiveViews[0] != nil {
		for k, v := range s.ActiveViews[0].KeyValues {
			view.KeyValues[k] = v
		}
	}

	s.ActiveViews = append(s.ActiveViews, view)
	return view
}

// GetViewIndex
// Returns the view index for a view.  Returns 0 if view is closed.
func (s *KVView) GetViewIndex(view *View) int {
	if view.Closed || len(s.ActiveViews) == 0 {
		return 0
	}

	// Look for and mark all the views that have timed out
	for _, v := range s.ActiveViews[1:] {
		if v.Closed {
			return 0
		}
		if dt := time.Since(v.LastAccess); dt > s.Timeout {
			v.Closed = true
		}
	}

	// First clear ActiveViews if no active view exists
	if len(s.ActiveViews) == 2 && s.ActiveViews[1].Closed { // Clear ActiveViews if none exist
		s.ActiveViews = s.ActiveViews[:0]
		return 0
	}
	for s.ActiveViews[1].Closed { // While the oldest ActiveView is closed, delete it
		n := len(s.ActiveViews)
		if n <= 2 { // If Removing the last view, clear ActiveViews
			s.ActiveViews = s.ActiveViews[:0] // No index exists for the View
			return 0
		}
		copy(s.ActiveViews[1:], s.ActiveViews[2:]) // Remove the closed View
		s.ActiveViews = s.ActiveViews[:n-1]
	}

	for i, v := range s.ActiveViews[1:] { // Look for the view in the Views that remain
		if v.ID == view.ID {
			return i + 1
		}
	}

	return 0
}

// ViewGet
// To a get of a key value pair using a view.  The view is searched first, and all
// active views that were created before this view are searched in turn.  If no
// key value pair is found in the view or older views, then return what the DB has
func (s *KVView) ViewGet(view *View, key [32]byte) (value []byte, err error) {
	//
	view.LastAccess = time.Now()
	// Check if the view provided is active.  If not, return an error that the
	// view has expired
	viewIdx := s.GetViewIndex(view)
	if viewIdx == 0 {
		return nil, fmt.Errorf("view invalid")
	}

	// Check the view and all the older views for a key value pair.  Note that even
	// if an older view has expired, we still need its key value pair rather than
	// the current key value pair.
	// IMPORTANT: Never check index 0. Index 0 is the cache with updates after all views.
	// Start from the view's index and go down to 1 (skip 0).
	for idx := viewIdx; idx >= 1; idx-- {
		if v, ok := s.ActiveViews[idx].KeyValues[key]; ok {
			return v, nil
		}
	}

	return s.DB.Get(key) // If no key value pair exists, return whatever the DB has.
}
