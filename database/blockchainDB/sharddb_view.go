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
}

// ShardDBViews
// A wrapper around a sharded DB that implements Views.  Views are created in a stack.
// More recent views lookup values in their cache, and then in the views that come
// before.  Later views are ignored.
type ShardDBViews struct {
	DB          *ShardDB      // The underlying DB
	ViewID      int           // The next ViewID
	ActiveViews []*View       // List of all active Views, newest first
	Map         map[int]View  // Fast lookup of a view
	Timeout     time.Duration // How long before views timeout; every access resets timeout
}

func NewShardDBViews(
	Directory string,
	Timeout time.Duration,
	Partition, ShardCnt,
	BufferCnt int) (sdbV *ShardDBViews, err error) {

	sdbV = new(ShardDBViews)
	sdbV.Timeout = Timeout
	if sdbV.DB, err = NewShardDB(Directory, Partition, ShardCnt, BufferCnt); err == nil {
		return sdbV, nil
	}
	return nil, err
}

func OpenShardDBViews(
	Directory string,
	Timeout time.Duration,
	Partition,
	BufferCnt int) (sdbV *ShardDBViews, err error) {

	sdbV = new(ShardDBViews)
	sdbV.Timeout = Timeout
	if sdbV.DB, err = OpenShardDB(
		Directory,
		Partition,
		BufferCnt); err == nil {
		return sdbV, nil
	}
	return nil, err
}

func (s *ShardDBViews) Close() {
	s.ActiveViews = nil
	s.Map = nil
	s.DB.Close()
}

// Active Views
// Returns true if a valid active view exists.  If old views
// exist, but none are active, the active views are tossed.
func (s *ShardDBViews) IsViewActive() bool {
	// The first entry in the ActiveViews is the View Cache
	// ActiveViews with a length less than two means no active views
	if len(s.ActiveViews) < 2 {
		return false
	}

	s.GetViewIndex(s.ActiveViews[1]) // This will clear ActiveViews if none are valid
	return len(s.ActiveViews) > 0    // If any remain, then a View is Active
}

func (s *ShardDBViews) Put(key [32]byte, value []byte) error {

	// If not view is active, then write to the DB
	if !s.IsViewActive() {
		return s.DB.Put(key, value)
	}

	s.ActiveViews[len(s.ActiveViews)-1].KeyValues[key] = value // Put key/value in last view
	s.ActiveViews[0].KeyValues[key] = value                    // And put the key value in the cache
	return nil
}

func (s *ShardDBViews) Get(key [32]byte) (value []byte) {

	// If no view is active, just get the DB value
	if !s.IsViewActive() {
		return s.DB.Get(key)
	}

	// If a view is active, we need to check the view cache first
	value, ok := s.ActiveViews[0].KeyValues[key]
	if ok {
		return value
	}

	return s.DB.Get(key) // Nothing in the view cache? Pull from the DB
}

func (s *ShardDBViews) NewView() *View {
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
	view.ID = s.ViewID
	view.LastAccess = time.Now()
	view.KeyValues = make(map[[32]byte][]byte)
	s.ActiveViews = append(s.ActiveViews, view)
	return view
}

// GetViewIndex
// Returns the view index for a view.  Returns 0 if no valid view exists
func (s *ShardDBViews) GetViewIndex(view *View) int {
	if len(s.ActiveViews) == 0 {
		return 0
	}

	// Look for and mark all the views that have timed out
	for _, v := range s.ActiveViews[1:] {
		if v.Closed {
			return 0
		}
		if dt := time.Since(view.LastAccess); dt > s.Timeout {
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
func (s *ShardDBViews) ViewGet(view *View, key [32]byte) (value []byte, err error) {
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
	// the current key value pair
	for viewIdx--; viewIdx > 0; viewIdx-- {
		if v, ok := s.ActiveViews[viewIdx].KeyValues[key]; ok {
			return v, nil
		}
	}

	return s.DB.Get(key), nil // If no key value pair exists, return whatever the DB has.
}
