package blockchainDB

import (
	"fmt"
	"sync"
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
	Mutex       sync.Mutex    // Serializes access; KVView methods are safe for concurrent use
	DB          *KVShard      // The underlying DB
	ViewID      int           // The next ViewID
	ActiveViews []*View       // List of all active Views, newest first
	Map         map[int]View  // Fast lookup of a view
	Timeout     time.Duration // How long before views timeout; every access resets timeout
	OffsetCnt   uint64        // KeyOffset number for the key files
	KeyLimit    uint64        // KeyLimit sets when to move keys to History
	FlushErr    error         // First error hit while flushing buffered writes to the DB
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

// flushViewCache
// Writes the key/value pairs buffered in the view cache (ActiveViews[0])
// into the underlying DB.  While views are active, Put only buffers
// writes; they land in the DB here, when the last view goes away.
// The first flush error is retained in s.FlushErr and returned by Close.
func (s *KVView) flushViewCache() {
	if len(s.ActiveViews) == 0 {
		return
	}
	for key, value := range s.ActiveViews[0].KeyValues {
		if err := s.DB.Put(key, value); err != nil && s.FlushErr == nil {
			s.FlushErr = err
		}
	}
	s.ActiveViews[0].KeyValues = nil
}

func (s *KVView) Close() error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.flushViewCache()
	s.ActiveViews = nil
	s.Map = nil
	err := s.DB.Close()
	if s.FlushErr != nil {
		return s.FlushErr
	}
	return err
}

// Active Views
// Returns true if a valid active view exists.  If old views
// exist, but none are active, the active views are tossed.
func (s *KVView) IsViewActive() bool {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.isViewActive()
}

// isViewActive
// Internal version of IsViewActive; the caller must hold the Mutex.
func (s *KVView) isViewActive() bool {
	// The first entry in the ActiveViews is the View Cache
	// ActiveViews with a length less than two means no active views
	if len(s.ActiveViews) < 2 {
		return false
	}

	s.getViewIndex(s.ActiveViews[1]) // This will clear ActiveViews if none are valid
	return len(s.ActiveViews) > 0    // If any remain, then a View is Active
}

func (s *KVView) Put(key [32]byte, value []byte) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// If not view is active, then write to the DB
	if !s.isViewActive() {
		return s.DB.Put(key, value)
	}

	s.ActiveViews[len(s.ActiveViews)-1].KeyValues[key] = value  // Put key/value in last view
	s.ActiveViews[len(s.ActiveViews)-1].LastAccess = time.Now() // Writing counts as activity
	s.ActiveViews[0].KeyValues[key] = value                     // And put the key value in the cache
	return nil
}

func (s *KVView) Get(key [32]byte) (value []byte, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// If no view is active, just get the DB value
	if !s.isViewActive() {
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
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// If no view is active, we have to cache DB updates so
	// create a "DB Update View" at s.ActiveViews[0]
	if !s.isViewActive() {
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
	s.ActiveViews = append(s.ActiveViews, view)
	return view
}

// GetViewIndex
// Returns the view index for a view.  Returns 0 if view is closed.
// Also expires views that have not been accessed within the timeout,
// prunes closed views from the old end of the stack, and flushes the
// buffered writes to the DB when the last view goes away.
func (s *KVView) GetViewIndex(view *View) int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.getViewIndex(view)
}

// getViewIndex
// Internal version of GetViewIndex; the caller must hold the Mutex.
func (s *KVView) getViewIndex(view *View) int {
	if len(s.ActiveViews) == 0 {
		return 0
	}

	// Mark all the views that have timed out.  Note this checks each
	// view's own LastAccess (checking the queried view's LastAccess here
	// would close every other view as soon as one view went stale).
	for _, v := range s.ActiveViews[1:] {
		if time.Since(v.LastAccess) > s.Timeout {
			v.Closed = true
		}
	}

	// Remove closed views from the old end of the stack.  Closed views in
	// the middle must be retained: newer views still look up buffered
	// writes through them.
	for len(s.ActiveViews) >= 2 && s.ActiveViews[1].Closed {
		n := len(s.ActiveViews)
		if n <= 2 { // Removing the last view: flush buffered writes, clear all
			s.flushViewCache()
			s.ActiveViews = s.ActiveViews[:0]
			break
		}
		copy(s.ActiveViews[1:], s.ActiveViews[2:]) // Remove the closed View
		s.ActiveViews[n-1] = nil
		s.ActiveViews = s.ActiveViews[:n-1]
	}

	if view.Closed {
		return 0
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
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Check if the view provided is active.  If not, return an error that
	// the view has expired.  Only refresh LastAccess for a still-valid
	// view; refreshing first would resurrect views that already expired.
	viewIdx := s.getViewIndex(view)
	if viewIdx == 0 {
		return nil, fmt.Errorf("view invalid")
	}
	view.LastAccess = time.Now()

	// Check the view and all the older views for a key value pair.  Note that even
	// if an older view has expired, we still need its key value pair rather than
	// the current key value pair
	for viewIdx--; viewIdx > 0; viewIdx-- {
		if v, ok := s.ActiveViews[viewIdx].KeyValues[key]; ok {
			return v, nil
		}
	}

	return s.DB.Get(key) // If no key value pair exists, return whatever the DB has.
}
