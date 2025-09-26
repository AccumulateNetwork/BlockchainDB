# Critical Bugs in View Implementation

## Bug 1: Wrong View Being Checked in GetViewIndex (Line 163)
**Location**: `view_kv.go:163`

### The Bug
```go
func (s *KVView) GetViewIndex(view *View) int {
    // ...
    for _, v := range s.ActiveViews[1:] {
        if v.Closed {
            return 0
        }
        if dt := time.Since(view.LastAccess); dt > s.Timeout {  // BUG: Should be v.LastAccess
            v.Closed = true
        }
    }
```

**Problem**: The timeout check uses `view.LastAccess` (the parameter) instead of `v.LastAccess` (the loop variable). This means:
- ALL views get their timeout checked against the SAME view's LastAccess time
- If the input view is old, ALL views get marked as closed
- If the input view is new, NO views get marked as closed

**Should be**:
```go
if dt := time.Since(v.LastAccess); dt > s.Timeout {
```

## Bug 2: IsViewActive Logic Error (Lines 93-98)
**Location**: `view_kv.go:93-98`

### The Bug
```go
func (s *KVView) IsViewActive() bool {
    if len(s.ActiveViews) < 2 {
        return false
    }
    s.GetViewIndex(s.ActiveViews[1])  // This will clear ActiveViews if none are valid
    return len(s.ActiveViews) > 0     // BUG: Should check > 1
}
```

**Problem**: After calling `GetViewIndex`, it checks if `len(s.ActiveViews) > 0`, but:
- ActiveViews[0] is the "DB Update View" (cache), not a real user view
- Should return true only if there are REAL views (len > 1)
- Currently returns true even with only the cache view

## Bug 3: Put Method Uses Wrong Index (Line 108)
**Location**: `view_kv.go:108`

### The Bug
```go
func (s *KVView) Put(key [32]byte, value []byte) error {
    if !s.IsViewActive() {
        return s.DB.Put(key, value)
    }
    s.ActiveViews[len(s.ActiveViews)-1].KeyValues[key] = value  // Put in LAST view
    s.ActiveViews[0].KeyValues[key] = value                     // Put in cache
    return nil
}
```

**Problem**: When views are active, Put() stores values in:
1. The LAST view (newest) - but this pollutes user views with new data!
2. The cache (index 0)

This breaks view isolation - new Puts should ONLY go to the cache (index 0), not to user views.

## Bug 4: ViewGet Searches Wrong Direction (Line 209)
**Location**: `view_kv.go:209`

### The Bug
```go
func (s *KVView) ViewGet(view *View, key [32]byte) (value []byte, err error) {
    viewIdx := s.GetViewIndex(view)
    // ...
    for viewIdx--; viewIdx > 0; viewIdx-- {  // Searches BACKWARDS from view
        if v, ok := s.ActiveViews[viewIdx].KeyValues[key]; ok {
            return v, nil
        }
    }
```

**Problem**: The loop searches from the view backwards to index 1, but:
- It skips index 0 (the cache with recent updates)
- A view should see all data that existed when it was created
- The cache (index 0) contains updates made after the first view was created

## Bug 5: Close() Doesn't Mark Views as Closed
**Location**: `view_kv.go:81-85`

### The Bug
```go
func (s *KVView) Close() {
    s.ActiveViews = nil  // Just nulls the slice
    s.Map = nil
    s.DB.Close()
}
```

**Problem**:
- Doesn't iterate through views to mark them as `Closed = true`
- Views retain references to the KVView and try to use it after Close()
- Results in nil pointer dereferences or invalid operations

## Bug 6: NewView Creates Extra View
**Location**: `view_kv.go:129-148`

This creates an extra "DB Update View" when no views exist, leading to unexpected view counts. While arguably a design choice, it confuses users and tests.

## Summary of Impact

These bugs cause:
1. **Incorrect timeout behavior** - views timeout based on wrong timestamp
2. **View pollution** - new data leaks into existing views
3. **Missing recent updates** - views don't see cache updates
4. **Resource leaks** - views not properly closed
5. **Test failures** - unexpected behavior throughout

## The Correct Flow Should Be:

1. **Views are snapshots**: Created at a point in time, see all data up to that point
2. **Updates after view creation**: Go only to cache (index 0), not to user views
3. **View reads**: Check the view's cache, older views, cache (index 0), then DB
4. **Timeout checks**: Each view checked against its own LastAccess
5. **Proper cleanup**: Close() marks all views as closed

These bugs explain why the TestView test fails - the implementation has fundamental issues with view isolation, timeout handling, and data consistency.