# Filesystem state across snapshots

`/volume-state` is an opt-in, destructive probe for a **disposable writable
fixture directory**. It observes process memory and retained descriptors. It
does not enable external-volume golden restores or assert that a restore is safe.

Before capture, provision these small fixtures (each file at most 4096 bytes):

- `cached.txt`: distinguishable source content copied into process memory.
- `held.txt`: a file kept open and read before capture.
- `unlinked.txt`: a file opened, read, and unlinked by the probe before capture.
- `directory/`: a directory kept open and enumerated before capture.
- `negative.txt`: must be absent; priming performs a negative lookup.

Use different sentinel contents for the source, actor A and actor B. Keep
expected values in the test controller, outside the captured process.

1. POST `/volume-state?root=<absolute-fixture-path>&challenge=<unique-test-token>`.
   Expect 204. Priming unlinks `unlinked.txt`; it must be on writable scratch
   storage. A second POST returns 409, even after a successful memory restore.
2. GET `/volume-state` and record the response before capturing the snapshot.
   The response includes the challenge, an increasing read count, application
   memory (`cached`), a fresh read of `cached.txt` (`by_path`), retained file
   reads (`held`, `unlinked`), a fresh lookup (`negative`), and enumeration through
   the retained directory descriptor (`directory`). Errors are reported separately
   and must be asserted by the test; HTTP 200 alone is not success.
3. Restore the captured snapshot, then GET only. Do not POST to the clone.
   An unprimed process returns 409, so a cold fallback cannot silently pass this
   continuity check. Require the original challenge and a count greater than
   the pre-capture count. Independently verify the source snapshot ID and runtime
   restore path; a live source process also retains the challenge.
4. After an own-snapshot capture, check the challenge and count again. This
   catches loss of probe state in second-generation captures.
5. DELETE `/volume-state` closes descriptors and clears state. It does not
   recreate the unlinked file. It returns 204 even when already unprimed.

For substitution experiments, seed the target backing tree from the host while
workloads are stopped. Create `negative.txt`, use distinct `cached.txt` and
`held.txt` contents, and give `directory/` different entries. Preserve all raw
responses, including read errors. An open deleted file may not be reconstructible
by path. A successful VM resume, EIO, or continued source content is not evidence
of safe substitution. In particular, the `cached` field is a deliberate copy in
application memory: host path rewrites or kernel cache invalidation cannot turn
it into target content.

An empty production placeholder needs a separate negative-lookup-only test.
These populated fixtures intentionally stress a more demanding contract; they
must not be described as the existing empty-placeholder golden workflow.
This endpoint is not yet wired into a cluster test or the automatic golden
capture lifecycle. It is a building block for that test, not an end-to-end result.

The local tests replace files and directories on the host. They verify that the
probe distinguishes path reads, retained descriptors and application memory,
rejects unprimed reads/re-priming, cleans up failed priming, and serializes reads.
They do not test NFS, virtio-fs migration, mmap, read-only enforcement, actor
isolation or a restored VM. Those require separate runtime integration tests.
