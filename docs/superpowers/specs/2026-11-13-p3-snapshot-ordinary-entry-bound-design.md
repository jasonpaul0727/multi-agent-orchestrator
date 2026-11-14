# Narrow design draft: stream ordinary snapshot directory names

Status: BLOCKED. Do not implement or ship the staged changes below until a
separately reviewed design resolves destination ownership and cleanup across
snapshot_workspace and both launchers. A self-owned test showed a newly
introduced scan failure can route through pathname cleanup after a destination
replacement and remove the replacement. Launcher staging cleanup has the same
class of pathname risk based on code review; that outer case is not yet
independently reproduced. Checking the pathname identity once does not exclude
a later replacement. Retaining uncertain staging instead requires a reviewed
privacy, capacity and recovery policy. The saved experimental patch is not an
approved implementation. Full v1 validation remains in force.

The historical candidate was a correction to the existing max_entries
contract in snapshot_workspace, not a new quota for protected names, a global
resource policy, a production isolation proof, or completion of the v1 async
handoff resource gate. Each implementation stage requires independent review.

## Observed behavior and limits

In src/orchestrator/isolation/workspace.py:81-336, entries starts at one for
the source root. copy_directory uses sorted(os.listdir(source_fd)) before
add_entry; validate_protected_entries independently uses sorted(os.listdir
(source_fd)) to re-read all root names. Both can materialize every name before
the global limit is checked. A nested directory itself consumes one entry;
each ordinary file, directory and symlink consumes one. Only casefold-matching
root names .git, .maestro and .env are excluded from that count. Names under
a non-root directory with those spellings are ordinary entries.

Every root name, including excluded names, is recorded for later exact
namespace comparisons. All protected root entries are lstat'd without following
them before ANY ordinary sibling is copied, and their dev/inode pairs prevent
opening hardlinked secret aliases. Root metadata and the complete root name set
are checked before copying and again after; file, directory, symlink and mount
checks use anchored FDs and compare observations. Keep all of those checks.

Production callers include SystemdReadOnlyLauncher.launch, which uses snapshot
defaults max_entries=1,000,000, max_bytes=4 GiB, max_depth=256; and
overlay_launcher, which supplies 100,000, 256 MiB and depth 64. These are
existing logical limits; caller max_entries can be an arbitrary positive int.
Neither bound fixes total process memory, physical disk use, kernel allocation,
CPU or page I/O. No claim is made about production deployment or v2.

## Candidate ordinary-name streaming

The only new bound proposed here is the number of ordinary names retained
and reserved at the application level while enumerating on a verified
fd-scandir runtime. It is relative to the existing per-call max_entries and
its remaining allowance. It does not bound protected names, all names, name
bytes, iterator/kernel buffering, Python allocator overhead, total RSS,
CPU/time, disk, other runtimes or the broader async handoff Stage 0 resources.
No new quota or global authorization is added.

On runtimes where os.scandir is in os.supports_fd, use the already opened,
anchored directory FD and a context-managed iterator. Keep only entry.name
strings, never DirEntry objects for later security decisions; the names for a
descriptor are str and should be sorted with the same Python sorted order as
listdir, including POSIX surrogateescape. Security decisions still perform
fresh no-follow stat/open/readlink and fstat/mount checks, not cached DirEntry
type or stat information.

For each directory, read the single shared remaining count:
  remaining = max_entries - entries
where entries already counts the root and all previously *reserved* ordinary
names. On the streaming path, before charging OR storing any yielded ordinary
or protected name, validate it using the existing raw-name checks and compare
the exact raw
Python name string against a per-scan seen set. A repeated exact name denotes
unstable enumeration and fails closed; do not count it or append it. Never
deduplicate by casefold, Unicode normalization, inode, or a hash: distinct
raw names such as .git and .GIT must both remain visible to protected prepass
and root-name comparison. Names in different directories may coincide. An
overflow sentinel is checked for exact duplicate before quota classification,
but is not stored. This fails closed on duplicate observations under concurrent
mutation; behavior for an unstable directory may differ from the old scan.

Stream names and reserve one unit for each ordinary name at discovery;
store no more than remaining ordinary names and read at most the next ordinary
name as an overflow sentinel. Protected names encountered in between must still
be processed in full and are never charged to that allowance. This bound
covers only application-level ordinary-name retention, not buffering internal
to the OS or scandir iterator. If the sentinel appears, raise the existing redacted
WorkspaceBoundaryError entry-limit category without appending it or processing
later input. At root, protected names are detected with the existing casefold
comparison and do not charge the ordinary limit. A root containing only
protected names remains valid even if remaining is zero.

Reserve every ordinary name in a parent before descending into any of its
children. Remove the old per-entry charge so no entry is counted twice. This
prevents keeping up to an entire remaining-size name list at every recursion
level: parent sibling lists plus descendants are jointly charged against the
same global count. Name order after successful collection is sorted exactly as
before. Directory traversal order and successful output are unchanged for a
stable, well-formed tree within the existing limits.

At root retain the original full sorted name tuple, including protected names,
and perform the same protected inode/stat prepass and validation before copying
any siblings. Record the baseline number of ordinary root names separately.
For each later root revalidation, stream names and allow at most that baseline
ordinary count plus one ordinary sentinel, while collecting protected names
without a new quota. Apply exact-raw-name duplicate checks before recording
entries; never casefold-deduplicate. Sort all collected names and compare
exact strings to the original root_names. Do not charge revalidation names
against global entries again. Additional protected names are not quota
violations; the existing exact root-name comparison can reject an actual
namespace change. A same-count ordinary replacement is also detected by that
exact comparison. The ordinary sentinel does not bound how many protected
names or how many iterator-internal entries can be read; revalidation is not a
general namespace snapshot or proof against ABA between observations.

Do not impose max_entries+3, any hardcoded Unicode-variant number or any other
protected-name quota. Distinct root names can have the same casefold, including
.git and .GIT. Preserve every variant the running interpreter considers
protected, including future Unicode versions and supported surrogateescape
behavior. An extra protected name must never be rejected merely to enforce a
new numeric cap. This intentionally leaves protected-name storage and total
root-name storage outside the new guarantee; do not claim O(max_entries)
total memory or count.

Choose the enumeration mode ONCE, before the first directory scan, from
the static capability check os.scandir in os.supports_fd. Only if that check
is false may an explicitly selected legacy compatibility path use the existing
anchored os.listdir(dir_fd), with its existing fail-closed identity checks.
This path materializes the legacy list before Python can inspect duplicates,
has no streaming guarantee and must not reopen an unanchored pathname,
expand permissions, or be described as bounded. After listdir returns, check
exact raw-name duplicates before algorithmic charging/copying; reject repeated
observations as unstable. If anchored fd-listdir is also unavailable or
unsafe, fail closed. Choosing strict unsupported failure instead of the
legacy fallback is a compatibility decision for independent review.

If the initial static check selects fd-scandir, that selection is sticky for
the entire snapshot, including all nested scans and root revalidation. Any
OSError during iterator creation, next or close—including ENOENT, EINVAL and
ENOSYS—fails closed under the exception/cleanup rules below. An unexpected
NotImplementedError or capability inconsistency after selection also fails
closed with a fixed redacted new error. Do not dynamically switch to listdir,
retry through a different mode, reopen by pathname, or report success.
Directory identity changes, unstable duplicate names, root-name mismatch and
re-scan/offset inconsistencies likewise fail closed without fallback. An
ordinary in-function operation that returns EINTR must follow the same
fail-closed rule if it reaches Python as an exception; no additional retry
is introduced to turn uncertainty into a successful snapshot.

The implementation already requires Linux-style no-follow directory FDs and
/proc/self/fdinfo mount IDs; support claims must match actual tested runtimes.
A read-only probe on this WSL Linux/Python 3.12.3 found fd-scandir supported and
two complete scans of one FD returned identical names. That does not prove
behavior across kernels, filesystems, Python versions, descriptor offsets or
concurrent edits. Validate context-manager close does not close the caller FD,
repeated scans start correctly and identity remains anchored on each claimed
target. If an independent descriptor must be opened relative to an anchored
FD to avoid offset sharing, prove its identity and assess search-permission
and FD cost before using it; never reopen by pathname and silently weaken the
boundary. No claim of fd-scandir streaming on an unverified target.

A later controlled worktree-filesystem fixture localized an earlier errno 2:
after a directory rename succeeded, scandir and fstat on the ORIGINAL opened
directory FD both returned ENOENT, while a new pathname lookup showed the
same inode at its new name. Complete repeated scans and scans repeated after
early termination were normal in a stable directory. That fixture also could
not create .git and .GIT as distinct names. These observations apply only to
that tested worktree filesystem/runtime. Passing self-created /tmp fixtures
applies only to their tested environment. They do not prove stable FD
anchoring through rename on this or every actual deployment target, casefold
behavior of other filesystems, or physical storage continuity.

For a rename or replacement that makes the anchored descriptor, its scandir,
fstat, identity check or revalidation fail, fail closed. The current pathname
cleanup is unsafe under replacement; new scan failures must not trigger it
before a separately reviewed ownership and recovery design. Do not switch to
listdir, retry a
different mode, or reopen any pathname, even if a fresh pathname reports the
same inode: it does not repair the broken handle/TOCTOU guarantee. The stable
tree behavior must remain usable; do not classify an entire filesystem as
unsupported solely from one unstable-directory fixture. Before implementation,
use TDD to establish which existing source branches currently emit raw
OSError versus WorkspaceBoundaryError for fstat/identity errors; propose and
review any needed fixed redacted from-None classification rather than assume
it already exists or silently broaden exception handling. A target deployment
with unknown filesystem/runtime compatibility cannot be described as
production-verified.

## Compatibility and concurrency

For stable valid trees within limits, preserve accepted names, excluded
protected spellings, sorting, byte/depth rules, summary and target contents.
Over-limit trees remain rejected. The current implementation attempts cleanup
by pathname; that name may have been replaced, so neither ownership nor
successful cleanup is established. Early quota detection can change the reported error
precedence for input having both excess entries and another defect. Example:
max_entries=2, an earlier-sorted escaping symlink and a later ordinary name
currently report symlink during ordered traversal; streaming pre-reservation
may report entry-limit first. If exact error text, mutation timing or handling
of concurrently unstable trees must stay identical, this candidate is not
strictly behavior-identical: pause and resolve that compatibility requirement
in independent review. Neither implementation should promise a coherent
snapshot against arbitrary concurrent changes.

Maintain current root-name and protected-stat rechecks even if the initial
ordinary-name listing is bounded on verified fd-scandir. No early copy may occur before the protected inode prepass.
Do not convert symlink/no-follow/mount or hardlink checks into hints. A
concurrent mutation may cause WorkspaceBoundaryError but matching before/after
samples do not prove that no intermediate change occurred. The partial-target
cleanup currently uses shutil.rmtree(ignore_errors=True): tests can verify
cleanup of own fixtures on normal filesystems, but failures must not be
misreported as a universal guarantee that all partial bytes disappeared.

### Exception, close and cleanup precedence

A context-manager close can itself raise while a scan or another cleanup is
already failing. Preserve the exact selected exception object, attempt every
applicable self-owned iterator/FD close and eligible partial-target cleanup,
and decide what to propagate only after these attempts. Classify an exception
as control flow when it is a BaseException that is NOT an Exception. Thus
KeyboardInterrupt, SystemExit and GeneratorExit are control flow, whereas
WorkspaceBoundaryError, OSError and ordinary Exception are not.

Priority rules:
1. If the scan's primary failure is control flow, propagate that exact primary
   object even if a later cleanup raises another control-flow exception.
2. Otherwise, if any cleanup raises control flow, propagate the FIRST such
   exact cleanup object after attempting remaining cleanup. This includes an
   entry-limit WorkspaceBoundaryError or any other ordinary primary error
   followed by KeyboardInterrupt/SystemExit/GeneratorExit during close or
   partial cleanup: never hide the interruption behind the ordinary error.
3. Otherwise, preserve an existing ordinary primary error (including the
   established entry-limit category); ordinary cleanup Exception does not
   override it. With no primary, a newly introduced scandir close/setup/
   iteration capability or OSError fails closed with a fixed generic
   WorkspaceBoundaryError from None. Never report success.
4. After a selected primary/control-flow error, any further error contributes
   only a fixed non-sensitive note such as workspace_enumeration_close_failed
   or workspace_cleanup_failed. Do not embed exception class-specific
   messages, str/repr, private names, paths, content or raw traceback.
   An earlier ordinary error displaced by control flow may be represented
   only by a fixed note. Best-effort note insertion cannot mask the selected
   exception object.

Never retry closing a numeric FD after close was attempted: its number may be
reused. The cleanup bookkeeping covers initial and root-recheck iterators and
nested directory/root FDs. A secondary failure must not prevent attempts to
close other owned resources or attempt cleanup. The caller's still-owned FD is
not closed by an iterator helper. The exact chosen primary/control-flow object
is propagated; additional exceptions do not replace it.

All NEW scandir open/iteration/close OSError and any newly introduced
unsupported-capability errors use fixed messages and are raised from None, not
from raw OS exceptions. An unsupported fd-scandir that successfully takes the
anchored legacy compatibility path is not an error. Do not retain raw cleanup exceptions in a long-lived
object or expose them in a note. Handle exception state explicitly so implicit
finally/context-manager chaining cannot disclose a later secret-bearing
OSError in a formatted traceback. No-primary control-flow exceptions propagate
as themselves; the design never converts cancellation or interruption into
success. This is a promise about newly introduced scandir errors, not a claim
to repair or redact every pre-existing exception chain elsewhere.

Creating a destination does not prove that the pathname still names it during
cleanup. The existing shutil.rmtree(ignore_errors=True) can act on a replacement
or silently leave files. New scan failures must not reach that pathname cleanup
until an independently reviewed ownership and recovery design establishes safe
behavior. Tests can characterize cleanup on self-owned fixtures but cannot prove
concurrent safety; they can verify the call and
successful removal of own fixtures; an injected failure may show only that
all eligible cleanup was attempted, not that disk bytes disappeared. Root,
child and iterator descriptors should be closed once under their respective
ownership scopes; do not turn interrupt or close failure into a successful
snapshot. Maintain full privacy in public exception output, including formatted
traceback and launcher wrapper, for newly injected scandir failures.

## Staged work (historical candidates; blocked pending a new safety design)

Each stage requires a real RED for its precise new behavior, the smallest
implementation, GREEN and a scoped independent review before the coordinator
makes an ordinary push. No staged result may claim the whole problem fixed
until both enumeration sites and final regression are complete. If anchored
FD semantics or the agreed compatibility behavior cannot be justified, stop
and record BLOCKED; do not solve it by changing protected-name policy.

A. First ordinary-name bounded collection for initial traversal.
Candidate files: src/orchestrator/isolation/workspace.py and
tests/unit/isolation/test_workspace_snapshot.py. A guard iterator around a real
own test directory must expose exactly remaining+1 ordinary entries and fail
if consumed further; demonstrate old listdir behavior RED, then new behavior
fails at sentinel without touching later entries. Integrate global reservation
before recursion, preserve excluded-name prepass and sorting. Cover max_entries
exact/+1, root-only protected case variants and nested siblings/depth. This
stage still has the old root revalidation allocation: label it partial and
do not claim even the ordinary-name streaming goal complete.

B. Exact root revalidation with ordinary-name sentinel and security races.
Same source plus tests/unit/isolation/test_workspace_secret_boundary.py and
test_workspace_snapshot.py. RED the root recheck independently with a
controlled iterator and no application-level ordinary-name retention beyond
baseline ordinary count plus one ordinary sentinel. Protected names have no
additional quota. Implement re-scan without charging entries; verify repeated FD scanning,
name additions/deletions/same-size replacement, equal unchanged root and
protected inode changes. Existing secret/hardlink/symlink/mount tests must
continue to pass; callbacks and errors must not expose secret filenames/data.
No claim that observation equality eliminates TOCTOU/ABA.

### Mandatory acceptance matrix for A/B/C

* A/B: root and nested iterators yield the same exact raw name twice; fail
  closed before charging/storing its second appearance. Distinct protected
  casefold variants must both survive prepass and exact root comparison; do
  not collapse ordinary names or names in different directories. Test both
  initial scan and root recheck, with real Unicode examples on supported POSIX.
* Iterator setup, next and close each raise OSError containing a distinctive
  private path/name sentinel. If unsupported fd-scandir takes the anchored
  listdir compatibility path, test legacy success/failure without claiming
  streaming. If a newly introduced unsupported-capability error is necessary,
  give its underlying failure a sensitive sentinel. For newly introduced
  errors, format the entire public exception traceback and verify no injected
  sentinel, no raw OSError as displayed cause/context, and one fixed category. Exercise both direct snapshot
  and actual launcher/overlay error wrapping without starting a systemd unit;
  do not assert this sanitizes unrelated legacy exceptions.
* Static selection and sticky failures: force the capability check false
  before the first scan and verify the ONLY compatibility fallback uses
  listdir on the anchored FD and makes no streaming claim; if fd-listdir also
  lacks support, fail closed. Separately force the check true, then inject
  ENOENT, EINVAL and ENOSYS at scandir creation, next and close, including
  root recheck and nested scan; inject directory identity and re-scan mismatch
  too. For every injected post-selection fault, instrument anchored listdir
  and any pathname enumeration to assert neither is called as fallback or
  retry, no snapshot success is returned, eligible cleanup is attempted, and
  newly introduced error output/full traceback is redacted according to the
  priority rules. These tests do not claim an untested worktree filesystem or
  an unknown-stage errno 2 passed.
* Descriptor invalidation after rename: in an isolated fixture on each
  actually supported filesystem, open the directory FD, trigger rename at
  deterministic barriers, and observe scandir creation/next and fstat/identity
  separately. Where this platform reproduces ENOENT, require fail-closed,
  redacted expected classification, all owned cleanup attempts, zero listdir
  fallback, zero pathname reopen and no success even if the new path has the
  same inode. Where rename leaves the FD usable, preserve the existing identity
  and root-mutation checks; do not fake ENOENT or force a failing fixture.
  Independently verify stable complete/repeated and early-stopped/repeated
  scans still work. Case-distinct protected-name tests must use a fixture
  filesystem that can create them, or a clearly labeled synthetic iterator
  test; a case-insensitive fixture cannot demonstrate coexistence. These tests
  are not proof for an untested production target.
* Fault priority matrix, with object-identity assertions for each selected
  control-flow exception: (a) primary KeyboardInterrupt/SystemExit/
  GeneratorExit plus cleanup OSError and a second control-flow exception:
  primary control-flow object wins; (b) primary entry-limit or another ordinary
  Exception plus cleanup KeyboardInterrupt/SystemExit/GeneratorExit: first
  cleanup control-flow object wins, later cleanups still attempted; (c) no
  primary, first cleanup OSError then control flow: first control-flow object
  wins; (d) primary ordinary error plus ordinary cleanup errors: primary error
  remains; (e) multiple cleanup control-flow errors: preserve the first;
  (f) close-only OSError is a fixed fail-closed WorkspaceBoundaryError from
  None. Include target cleanup itself raising control flow. Every distinct
  owned close and eligible partial-target cleanup is attempted; no numeric FD
  close is retried. Non-selected errors produce only fixed notes. Inspect the
  full formatted traceback, not just str(error); no sentinel from a
  NON-SELECTED close/error or new wrapped OSError may appear. Give selected
  control-flow objects benign messages in this test: preserving their exact
  identity also preserves their own arguments, so this does not claim to redact
  arbitrary pre-existing selected exception messages. With injected
  shutil.rmtree failure or ignore_errors, check attempted cleanup and report
  uncertainty rather than asserting removal.
* Repeatable fd-scandir behavior and anchored descriptor ownership are hard
  gates for claiming the streaming ordinary-name guarantee on a specific
  runtime/filesystem. Test repeated scans and original FD usability. Verify
  dynamic casefold-protected variants remain exempt with no numeric limit,
  including additional protected names during revalidation; a real changed
  root still fails exact comparison. Test unsupported fallback uses only the
  anchored FD and explicitly carries no streaming guarantee. Never assert a
  protected-name/total-name bound or use a protected quota as a shortcut.

C. Real compatibility, fault and integration coverage.
First write a test for a concrete new boundary behavior still absent after A/B
and verify a genuine RED; implement the smallest corresponding fix and obtain
GREEN. If all proposed checks already pass, record them as characterization,
not a fabricated RED; a test-only change counts as substantive only if it is
independently meaningful under the coordinator's existing rules. If no third
substantive change exists, do not invent one or claim the required three pushes.
Candidate distinct real-filesystem checks: multiple excluded casefold
variants, raw invalid UTF-8 byte names on supported POSIX, deterministic
ordering, full global remaining across several branches, small entry cap with
many self-created entries, scandir failure at initial/mid/recheck/close,
iterator and FD cleanup, failure after partial copy and controlled root
mutation. Record rather than hide the coexisting-error precedence difference;
preserve accepted/rejected semantics. Exercise both real snapshot callers with
offline/own-fixture boundaries; do not start a production service. Review
failure/unsupported platform results and run targeted workspace snapshot,
secret boundary, launcher and overlay regression tests. Independent whole
review and fresh full suite at final HEAD remain required before marking the
three-stage work complete.

Tests use only self-created directories/FDs, finite guard iterators and bounded
owned processes. Never create huge trees or exhaust a shared filesystem.
Test-only guarded iterators are not proof of physical RSS/disk/CPU limits.
This narrow ordinary-name fix does not prove a protected-name or total-name
bound, iterator buffering/bytes/RSS, the broader async handoff Stage 0
capacity gate (which remains BLOCKED), SQLite authority, systemd stop,
durable receipts or any v2 permission.
