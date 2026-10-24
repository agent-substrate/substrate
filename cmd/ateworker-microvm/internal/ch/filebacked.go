// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
)

const (
	snapshotConfigFile      = "config.json"
	snapshotStateFile       = "state.json"
	memoryManagerSnapshotID = "memory-manager"
	defaultMemoryZone       = "mem0"
)

// ErrUnsupportedLayout indicates the snapshot's guest-RAM layout cannot be file-backed.
var ErrUnsupportedLayout = errors.New("snapshot guest memory cannot be file-backed")

// layoutError wraps a formatted message with ErrUnsupportedLayout.
func layoutError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedLayout, fmt.Sprintf(format, args...))
}

// readSnapshotMeta reads and parses config.json and state.json from a Cloud Hypervisor snapshot directory.
func readSnapshotMeta(dir string) (cfgPath, statePath string, cfgBytes []byte, cfg, mem jsonObject, memRaw json.RawMessage, state *memoryManagerState, err error) {
	cfgPath, statePath = filepath.Join(dir, snapshotConfigFile), filepath.Join(dir, snapshotStateFile)
	cfgBytes, err1 := os.ReadFile(cfgPath)
	stateBytes, err2 := os.ReadFile(statePath)
	if err = errors.Join(err1, err2); err != nil {
		return
	}
	if cfg, err = parseObject(cfgBytes); err != nil {
		err = fmt.Errorf("parsing %s: %w", cfgPath, err)
		return
	}
	memRaw, ok := cfg.get("memory")
	if !ok {
		err = layoutError("config.json has no memory section")
		return
	}
	if mem, err = parseObject(memRaw); err != nil {
		err = layoutError("config.json memory section: %v", err)
		return
	}
	state, err = parseMemoryManagerState(stateBytes)
	return
}

// UseFileBackedMemory rewrites the memfd snapshot in snapshotDir so vm.restore maps guest
// RAM directly from file and empties the memory-manager memory_ranges table.
func UseFileBackedMemory(snapshotDir, file string) (origMemory json.RawMessage, err error) {
	cfgPath, statePath, cfgBytes, cfg, mem, memRaw, state, err := readSnapshotMeta(snapshotDir)
	if err != nil {
		return nil, err
	}
	memSize, err := memfdMemorySize(mem)
	if err != nil {
		return nil, err
	}
	mappings, err1 := state.mappings()
	ranges, err2 := state.ranges()
	if err := errors.Join(err1, err2); err != nil {
		return nil, layoutError("%v", err)
	}
	end, err := validateMemfdLayout(mappings, ranges, memSize)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(file)
	if err != nil {
		return nil, fmt.Errorf("stat guest memory file: %w", err)
	}
	if !fi.Mode().IsRegular() || uint64(fi.Size()) < end {
		return nil, layoutError("guest memory file %q invalid or too small (size=%d want=%d)", file, fi.Size(), end)
	}

	zone, err := fileBackedZone(mem, file)
	if err != nil {
		return nil, err
	}
	newMem := mem.clone()
	newMem.set("size", json.RawMessage("0"))
	newMem.setBefore("zones", zone, "thp")
	newMemRaw, err := newMem.marshal()
	if err != nil {
		return nil, err
	}
	newCfg, err := cfg.setMarshal("memory", newMemRaw)
	if err != nil {
		return nil, err
	}
	newState, err := state.withRanges(nil)
	if err != nil {
		return nil, err
	}
	if err := writeFileAtomic(cfgPath, newCfg); err != nil {
		return nil, err
	}
	if err := writeFileAtomic(statePath, newState); err != nil {
		_ = writeFileAtomic(cfgPath, cfgBytes)
		return nil, fmt.Errorf("writing %s: %w", statePath, err)
	}
	return append(json.RawMessage(nil), memRaw...), nil
}

// CanonicalizeFileBackedSnapshot restores a file-backed snapshot in dir back to canonical memfd format.
func CanonicalizeFileBackedSnapshot(dir string, origMemory json.RawMessage) error {
	cfgPath, statePath, _, cfg, mem, memRaw, state, err := readSnapshotMeta(dir)
	if err != nil {
		return err
	}
	var canonicalMem json.RawMessage
	if zoneSize, fileBacked, err := fileBackedZoneSize(mem); err != nil {
		return err
	} else if fileBacked {
		if canonicalMem, err = canonicalMemorySection(origMemory, mem, zoneSize); err != nil {
			return err
		}
	} else {
		if origMemory != nil && !jsonEqual(memRaw, origMemory) {
			return fmt.Errorf("%s memory section is neither file-backed nor the original one", cfgPath)
		}
		if _, err := memfdMemorySize(mem); err != nil {
			return err
		}
	}

	mappings, err1 := state.mappings()
	ranges, err2 := state.ranges()
	if err := errors.Join(err1, err2); err != nil {
		return err
	}
	want, err := memfdRanges(mappings)
	if err != nil {
		return err
	}
	if len(ranges) == 0 {
		newState, err := state.withRanges(want)
		if err != nil {
			return err
		}
		if err := writeFileAtomic(statePath, newState); err != nil {
			return err
		}
	} else if !slices.Equal(ranges, want) {
		return fmt.Errorf("%s memory_ranges %v match neither empty nor guest RAM mappings %v", statePath, ranges, want)
	}
	if canonicalMem != nil {
		newCfg, err := cfg.setMarshal("memory", canonicalMem)
		if err != nil {
			return err
		}
		return writeFileAtomic(cfgPath, newCfg)
	}
	return nil
}

// memfdMemorySize validates a canonical anonymous-memfd memory configuration and returns its byte size.
func memfdMemorySize(mem jsonObject) (uint64, error) {
	sizeRaw, ok := mem.get("size")
	if !ok {
		return 0, layoutError("memory.size is missing")
	}
	size, err := parseUint(sizeRaw)
	if err != nil || size == 0 {
		return 0, layoutError("memory.size invalid or 0 (%v)", err)
	}
	for _, key := range []string{"zones", "hotplug_size", "hotplugged_size", "hugepage_size"} {
		if v, ok := mem.get(key); ok && !isNull(v) {
			return 0, layoutError("memory.%s is set (%s)", key, v)
		}
	}
	shared, err1 := optBool(mem, "shared")
	huge, err2 := optBool(mem, "hugepages")
	if err := errors.Join(err1, err2); err != nil || !shared || huge {
		return 0, layoutError("memory.shared=%v memory.hugepages=%v (err=%v)", shared, huge, err)
	}
	return size, nil
}

// fileBackedZone constructs a single-element zones JSON array mapping defaultMemoryZone to file.
func fileBackedZone(mem jsonObject, file string) (json.RawMessage, error) {
	fileRaw, err := marshalNoEscape(file)
	if err != nil {
		return nil, err
	}
	sizeRaw, _ := mem.get("size")
	zone := jsonObject{
		{"id", json.RawMessage(`"` + defaultMemoryZone + `"`)},
		{"size", sizeRaw},
		{"file", fileRaw},
		{"shared", json.RawMessage("true")},
		{"hugepages", json.RawMessage("false")},
		{"prefault", json.RawMessage("false")},
	}
	for _, key := range []string{"reserve", "mergeable"} {
		b, err := optBool(mem, key)
		if err != nil {
			return nil, layoutError("memory.%s: %v", key, err)
		}
		zone = append(zone, jsonField{key, json.RawMessage(strconv.FormatBool(b))})
	}
	z, err := zone.marshal()
	return json.RawMessage("[" + string(z) + "]"), err
}

// fileBackedZoneSize inspects mem and reports the backing zone size when configured for file-backed guest RAM.
func fileBackedZoneSize(mem jsonObject) (size uint64, fileBacked bool, err error) {
	sizeRaw, ok := mem.get("size")
	if !ok {
		return 0, false, fmt.Errorf("memory.size is missing")
	}
	if s, err := parseUint(sizeRaw); err != nil || s != 0 {
		return 0, false, err
	}
	zonesRaw, ok := mem.get("zones")
	if !ok || isNull(zonesRaw) {
		return 0, false, fmt.Errorf("memory.size is 0 but there are no zones")
	}
	var zones []struct {
		ID   string  `json:"id"`
		Size uint64  `json:"size"`
		File *string `json:"file"`
	}
	if err := json.Unmarshal(zonesRaw, &zones); err != nil {
		return 0, false, err
	}
	if len(zones) != 1 || zones[0].ID != defaultMemoryZone || zones[0].File == nil || *zones[0].File == "" || zones[0].Size == 0 {
		return 0, false, fmt.Errorf("memory.zones is not one file-backed %q zone: %s", defaultMemoryZone, zonesRaw)
	}
	return zones[0].Size, true, nil
}

// canonicalMemorySection restores the canonical memfd memory JSON object from origMemory or derives it from fileBacked.
func canonicalMemorySection(origMemory json.RawMessage, fileBacked jsonObject, zoneSize uint64) (json.RawMessage, error) {
	if origMemory == nil {
		m := fileBacked.clone()
		m.set("size", json.RawMessage(strconv.FormatUint(zoneSize, 10)))
		m.remove("zones")
		return m.marshal()
	}
	orig, err := parseObject(origMemory)
	if err != nil {
		return nil, err
	}
	if size, err := memfdMemorySize(orig); err != nil || size != zoneSize {
		return nil, errors.Join(err, fmt.Errorf("original memory size %d != zone size %d", size, zoneSize))
	}
	return append(json.RawMessage(nil), origMemory...), nil
}

// validateMemfdLayout verifies that guest_ram_mappings and memory_ranges form a contiguous mem0 layout of memSize bytes.
func validateMemfdLayout(mappings []guestRAMMapping, ranges []memoryRange, memSize uint64) (uint64, error) {
	if len(mappings) == 0 {
		return 0, layoutError("no guest RAM mappings")
	}
	var total, end uint64
	for _, m := range mappings {
		if m.ZoneID != defaultMemoryZone || m.VirtioMem || m.Size == 0 || total+m.Size < total || m.FileOffset+m.Size < m.FileOffset {
			return 0, layoutError("invalid guest RAM mapping at gpa %#x", m.GPA)
		}
		total += m.Size
		end = max(end, m.FileOffset+m.Size)
	}
	if total != memSize || len(ranges) != len(mappings) {
		return 0, layoutError("mappings/ranges mismatch (total=%d memSize=%d ranges=%d mappings=%d)", total, memSize, len(ranges), len(mappings))
	}
	used := make([]bool, len(mappings))
	var cursor uint64
	for _, r := range ranges {
		i := slices.IndexFunc(mappings, func(m guestRAMMapping) bool { return m.GPA == r.GPA && m.Size == r.Length })
		if i < 0 || used[i] || mappings[i].FileOffset != cursor {
			return 0, layoutError("memory_ranges entry {gpa %#x, length %d} mismatch at cursor %d", r.GPA, r.Length, cursor)
		}
		used[i] = true
		cursor += r.Length
	}
	return end, nil
}

// memfdRanges reconstructs the ordered memory_ranges descriptors from guest_ram_mappings.
func memfdRanges(mappings []guestRAMMapping) ([]memoryRange, error) {
	ms := append([]guestRAMMapping(nil), mappings...)
	sort.SliceStable(ms, func(i, j int) bool { return ms[i].FileOffset < ms[j].FileOffset })
	var out []memoryRange
	var cursor uint64
	for _, m := range ms {
		if m.VirtioMem || m.ZoneID != defaultMemoryZone || m.Size == 0 || m.FileOffset != cursor || cursor+m.Size < cursor {
			return nil, fmt.Errorf("invalid guest RAM mapping at gpa %#x", m.GPA)
		}
		out = append(out, memoryRange{GPA: m.GPA, Length: m.Size})
		cursor += m.Size
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no guest RAM mappings")
	}
	return out, nil
}

// guestRAMMapping represents an entry in Cloud Hypervisor's guest_ram_mappings state array.
type guestRAMMapping struct {
	Slot       uint32 `json:"slot"`
	GPA        uint64 `json:"gpa"`
	Size       uint64 `json:"size"`
	ZoneID     string `json:"zone_id"`
	VirtioMem  bool   `json:"virtio_mem"`
	FileOffset uint64 `json:"file_offset"`
}

// memoryRange represents an entry in Cloud Hypervisor's memory_ranges.data state array.
type memoryRange struct {
	GPA    uint64 `json:"gpa"`
	Length uint64 `json:"length"`
}

// memoryManagerState holds the parsed JSON layers of Cloud Hypervisor's state.json memory-manager snapshot.
type memoryManagerState struct {
	top, snapshots, mm, data, state jsonObject
}

// parseMemoryManagerState parses the nested memory-manager state object from state.json bytes.
func parseMemoryManagerState(b []byte) (*memoryManagerState, error) {
	var s memoryManagerState
	var err1, err2, err3, err4 error
	s.top, err1 = parseObject(b)
	s.snapshots, err2 = s.top.object("snapshots")
	s.mm, err3 = s.snapshots.object(memoryManagerSnapshotID)
	s.data, err4 = s.mm.object("snapshot_data")
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return nil, layoutError("state.json: %v", err)
	}
	stateRaw, ok := s.data.get("state")
	if !ok {
		return nil, layoutError("state.json %s has no state", memoryManagerSnapshotID)
	}
	var stateStr string
	if err := json.Unmarshal(stateRaw, &stateStr); err != nil {
		return nil, layoutError("state.json %s state: %v", memoryManagerSnapshotID, err)
	}
	if s.state, err1 = parseObject([]byte(stateStr)); err1 != nil {
		return nil, layoutError("state.json %s state: %v", memoryManagerSnapshotID, err1)
	}
	return &s, nil
}

func (s *memoryManagerState) mappings() ([]guestRAMMapping, error) {
	raw, ok := s.state.get("guest_ram_mappings")
	if !ok {
		return nil, fmt.Errorf("no guest_ram_mappings")
	}
	var ms []guestRAMMapping
	return ms, decodeStrict(raw, &ms)
}

func (s *memoryManagerState) ranges() ([]memoryRange, error) {
	raw, ok := s.state.get("memory_ranges")
	if !ok {
		return nil, fmt.Errorf("no memory_ranges")
	}
	var t struct {
		Data []memoryRange `json:"data"`
	}
	return t.Data, decodeStrict(raw, &t)
}

func (s *memoryManagerState) withRanges(ranges []memoryRange) ([]byte, error) {
	var t bytes.Buffer
	t.WriteString(`{"data":[`)
	for i, r := range ranges {
		if i > 0 {
			t.WriteByte(',')
		}
		fmt.Fprintf(&t, `{"gpa":%d,"length":%d}`, r.GPA, r.Length)
	}
	t.WriteString(`]}`)
	stateJSON, err := s.state.setMarshal("memory_ranges", t.Bytes())
	if err != nil {
		return nil, err
	}
	stateStr, err := marshalNoEscape(string(stateJSON))
	if err != nil {
		return nil, err
	}
	dataRaw, err1 := s.data.setMarshal("state", stateStr)
	mmRaw, err2 := s.mm.setMarshal("snapshot_data", dataRaw)
	snapsRaw, err3 := s.snapshots.setMarshal(memoryManagerSnapshotID, mmRaw)
	out, err4 := s.top.setMarshal("snapshots", snapsRaw)
	return out, errors.Join(err1, err2, err3, err4)
}

// jsonObject is an order-preserving JSON object representation.
type jsonObject []jsonField

type jsonField struct {
	key string
	val json.RawMessage
}

// parseObject decodes a JSON object while preserving field order and rejecting duplicate keys or trailing tokens.
func parseObject(b []byte) (jsonObject, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("not a JSON object")
	}
	obj, seen := jsonObject{}, map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok || seen[key] {
			return nil, fmt.Errorf("invalid or duplicate key %v", tok)
		}
		seen[key] = true
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
		obj = append(obj, jsonField{key, val})
	}
	_, err1 := dec.Token()
	_, err2 := dec.Token()
	if err1 != nil || err2 != io.EOF {
		return nil, fmt.Errorf("trailing tokens after JSON object")
	}
	return obj, nil
}

func (o jsonObject) get(key string) (json.RawMessage, bool) {
	for _, f := range o {
		if f.key == key {
			return f.val, true
		}
	}
	return nil, false
}

func (o jsonObject) object(key string) (jsonObject, error) {
	raw, ok := o.get(key)
	if !ok || isNull(raw) {
		return nil, fmt.Errorf("no %q object", key)
	}
	return parseObject(raw)
}

func (o jsonObject) clone() jsonObject { return append(jsonObject(nil), o...) }

func (o *jsonObject) set(key string, val json.RawMessage) {
	for i := range *o {
		if (*o)[i].key == key {
			(*o)[i].val = val
			return
		}
	}
	*o = append(*o, jsonField{key, val})
}

func (o jsonObject) setMarshal(key string, val json.RawMessage) ([]byte, error) {
	c := o.clone()
	c.set(key, val)
	return c.marshal()
}

func (o *jsonObject) setBefore(key string, val json.RawMessage, before string) {
	if _, ok := o.get(key); ok {
		o.set(key, val)
		return
	}
	for i := range *o {
		if (*o)[i].key == before {
			*o = append((*o)[:i], append(jsonObject{{key, val}}, (*o)[i:]...)...)
			return
		}
	}
	*o = append(*o, jsonField{key, val})
}

func (o *jsonObject) remove(key string) {
	*o = slices.DeleteFunc(*o, func(f jsonField) bool { return f.key == key })
}

func (o jsonObject) marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, f := range o {
		if i > 0 {
			buf.WriteByte(',')
		}
		k, err := marshalNoEscape(f.key)
		if err != nil {
			return nil, err
		}
		buf.Write(k)
		buf.WriteByte(':')
		buf.Write(f.val)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func marshalNoEscape(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(v)
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), err
}

func decodeStrict(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func isNull(raw json.RawMessage) bool                  { return string(bytes.TrimSpace(raw)) == "null" }
func parseUint(raw json.RawMessage) (uint64, error)   { return strconv.ParseUint(string(bytes.TrimSpace(raw)), 10, 64) }
func optBool(o jsonObject, key string) (bool, error) {
	raw, ok := o.get(key)
	if !ok || isNull(raw) {
		return false, nil
	}
	var b bool
	return b, json.Unmarshal(raw, &b)
}

func jsonEqual(a, b json.RawMessage) bool {
	var va, vb any
	da, db := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	da.UseNumber()
	db.UseNumber()
	if da.Decode(&va) != nil || db.Decode(&vb) != nil {
		return false
	}
	ja, errA := json.Marshal(va)
	jb, errB := json.Marshal(vb)
	return errA == nil && errB == nil && bytes.Equal(ja, jb)
}

// writeFileAtomic writes data to a temporary sibling file and atomically renames it over path.
func writeFileAtomic(path string, data []byte) error {
	mode := os.FileMode(0o600)
	if fi, err := os.Stat(path); err == nil {
		mode = fi.Mode().Perm()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err1 := tmp.Write(data)
	if err := errors.Join(err1, tmp.Chmod(mode), tmp.Close()); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
