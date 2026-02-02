package index

import (
	"maps"
	"sync"
)

// SymbolTable is a bidirectional mapping between strings and integer IDs
// This is used to compress repeated string values (like tag keys/values)
type SymbolTable struct {
	mu      sync.RWMutex
	strToID map[string]uint32
	idToStr map[uint32]string
	nextID  uint32
}

// NewSymbolTable creates a new symbol table
func NewSymbolTable() *SymbolTable {
	return &SymbolTable{
		strToID: make(map[string]uint32),
		idToStr: make(map[uint32]string),
		nextID:  1, // Start at 1, reserve 0 for null/empty
	}
}

// Intern returns the ID for a string, adding it if not present
func (st *SymbolTable) Intern(s string) uint32 {
	st.mu.RLock()
	if id, ok := st.strToID[s]; ok {
		st.mu.RUnlock()
		return id
	}
	st.mu.RUnlock()

	st.mu.Lock()
	defer st.mu.Unlock()

	// Double-check in case another goroutine added it
	if id, ok := st.strToID[s]; ok {
		return id
	}

	id := st.nextID
	st.nextID++
	st.strToID[s] = id
	st.idToStr[id] = s

	return id
}

// Lookup returns the ID for a string, or 0 if not found
func (st *SymbolTable) Lookup(s string) uint32 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.strToID[s]
}

// Resolve returns the string for an ID, or empty string if not found
func (st *SymbolTable) Resolve(id uint32) string {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.idToStr[id]
}

// Size returns the number of unique strings in the table
func (st *SymbolTable) Size() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.strToID)
}

// SerializeToMap exports the symbol table as a map for JSON serialization
func (st *SymbolTable) SerializeToMap() map[string]uint32 {
	st.mu.RLock()
	defer st.mu.RUnlock()

	result := make(map[string]uint32, len(st.strToID))
	maps.Copy(result, st.strToID)
	return result
}

// NewSymbolTableFromMap creates a symbol table from a serialized map
func NewSymbolTableFromMap(m map[string]uint32) *SymbolTable {
	st := &SymbolTable{
		strToID: make(map[string]uint32, len(m)),
		idToStr: make(map[uint32]string, len(m)),
		nextID:  1,
	}

	// Reconstruct both mappings and find the next ID
	for str, id := range m {
		st.strToID[str] = id
		st.idToStr[id] = str
		if id >= st.nextID {
			st.nextID = id + 1
		}
	}

	return st
}
