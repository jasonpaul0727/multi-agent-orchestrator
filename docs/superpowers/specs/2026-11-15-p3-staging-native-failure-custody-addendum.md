# Addendum: A2 native failure classification and retained custody

Status: FIX ROUND 3 / DESIGN FOR SCOPED REVIEW. Addresses all five findings in
staging-a2-addendum-design-review.md (SHA256
e7d558d6714e2afbd56067cc9fec70824e6a2e39176d34f738b1eb61f4446a7c).
No code restoration, implementation, native/helper/test invocation, release,
commit or second native acceptance is authorized or performed by this document.

## Boundary and preserved evidence

The entire rejected seven-file A2 increment stays parked. A1 contract/arena and
its tests remain accepted. The old receipt observed subreaper/pidfd and returned
UNSUPPORTED, but retained neither the precise failed operation nor errno; review
proved that internal failures could receive that label. Its exact cause remains
unknown. Do not fill historical fields, infer EPERM, or claim unsupported host
namespaces. Preserve receipt/report/archive and original arena.

Scope is repaired native-prerequisite A2 plus ONE separate fixed R/S/T
PRE_UNSHARE retention fixture and that new fixture's narrow release rule. Keep
profile, fixed /usr/bin/python3.12 -B -I -S loader, no discovery subprocesses,
16 KiB shared accounting, original 30 s + <=5 s cleanup allowance and <=9 process
limit. No Worker, data mount/copy, Provider, deployment, main merge or change to
full v1/unknown holds. Repaired native attempts have no release authority.

## 1. Concrete host anchor, acquisition debt and administration ownership

Supported entry is ONLY
orchestrator.isolation._staging_gate.NativeHostRoot.open_preserved(). Its module
has one strong `_HOST_ROOT` initialized in the module dictionary before any
controlled manifest/root/native acquisition. This is a minimal private root, not
a generic service framework. It owns a two-element pool, one non-reentrant
_thread lock, creator PID/thread/interpreter identity, lifecycle and debt flags.

Before VERIFY_MANIFEST, REOPEN_ROOTS, dynamic-loader/import validation or any
other fallible controlled FD acquisition, the root installs a BOOTSTRAP owner
placeholder under its lock. Initial allocation uses only built-in objects/the
built-in _thread lock. Even pre-reservation failures therefore have a root.
The placeholder transitions to a token-bound attempt only after reservation.

Composition checks are fixed:

- `__name__` is the canonical module name, sys.modules[name] is this module, and
  __spec__.origin is its exact trusted source path. Alternate-import execution
  refuses before acquiring controlled FDs.
- First module execution sets an immutable boot sentinel; reload sees it and
  raises before rebinding definitions/_HOST_ROOT. Never replace an existing root.
- PID equals creator PID; inherited post-fork host roots refuse. Installation and
  every public method run only on the recorded main-thread identity; other
  threads and same-thread reentry refuse, without waiting or starting work.
- In CPython 3.12.3, use the explicitly manifested standard-library
  _xxsubinterpreters module's get_current()==get_main() check; absent/unrecognized
  origin refuses. Load only its fixed interpreter-resolved builtin or declared
  /usr/lib/python3.12/lib-dynload object, never find_library/PATH. Import/load
  failure is inside BOOTSTRAP debt accounting, not namespace unsupported.

The lock protects root/pool state, admission claims, evidence mint/consume,
release-in-progress and close state. Never hold it across FD/native/A1 calls.
Mark the entry busy before leaving the lock; all other root mutation refuses
until that phase commits. There are no background threads/executors. `_HOST_ROOT`
retains entries after caller return/cancellation; caller/Future/weakref/destructor
lifetime is irrelevant. Root methods returning status provide no release rights.

Composition administration FDs are EXACTLY four validated existing directory
FDs (parent, staging-fixture, source, arena), and four independent SlotArena FDs
(arena directory, arena.header, slot-0.record, slot-1.record). They stay in the
root until explicit root.close; they are not passed to descendants as owners.
Each attempt has separate registered inherited-root duplicates, one shared-budget
FD, private endpoint/stdio ends, and exact pidfds for the finite roles. Acquire
and record each returned FD before the next fallible action. These attempt FDs
must close before minting fixture evidence; only the eight enumerated composition
metadata FDs remain to perform the release. No unspecified FD is exempted.

A1 constructors/manifest helpers are OPAQUE boundaries: they do not return a full
acquisition ledger on failure. Keep their accepted behavior; do not pretend the
caller learned which hidden FD acquisition/close succeeded. If such a helper
raises, or its close completion is uncertain, latch POISONED_UNRESERVED_DEBT (or
token-bound POISONED if already reserved), retain the placeholder/returned object
and all known handles, and refuse ALL subsequent root admission/release. No token
is needed for this debt. Do not mark an unrelated on-disk FREE record UNKNOWN
without a token, and do not claim an exact closed ledger for hidden allocations.
A successful opaque helper returns normally and its known returned FDs join the
root ledger. New A2-owned acquisitions use owner-aware records; no broad rewrite
of accepted A1 helpers/tests is implied. Debt cannot be cleared by a close flag,
new root object, reopen, lack of a reservation token or an old receipt.

root.close returns RETAINED without closing anything if an owner is live/parked,
busy, has acquisition debt or incomplete release. With all attempts closed it
attempts each administration close once; complete success returns CLOSED and
leaves a permanent strong tombstone. Uncertain close leaves POISONED and retains
the root/debt; a selected control-flow exception is propagated. No root reset or
second installation is allowed, even after CLOSED. Hard host death/reload outside
this supported entry or coherent all-state rollback remains unproved; no external
guardian is assumed. A poisoned bootstrap cannot earn acceptance as complete
native custody. This scope does not claim ownership of pre-existing host/import FDs.

## 2. Finite failure records and correct namespace ordering

Set phase before each operation. On a libc -1, sample ctypes.get_errno immediately;
on a checked os syscall OSError, sample that exception's actual errno immediately.
Validate exact call arguments before execution. Failure fields are exactly
`role:{HOST,R,S,T,C}`, `phase`, `kind`, `errno:null|int(1..4095)`; no bool-as-int,
raw message, dynamic class name, path or traceback. Kinds: NATIVE_REJECTION,
NATIVE_OPERATION_FAILURE, CONTRACT_FAILURE, INVARIANT_FAILURE, PROTOCOL_FAILURE,
INTERRUPTED, CLEANUP_UNKNOWN.

Finite phases are VALIDATE_REQUEST, VERIFY_MANIFEST, REOPEN_ROOTS, RESERVE_SLOT,
LOAD_LIBC, SPAWN_R, REGISTER_R, SET_SUBREAPER, VERIFY_SUBREAPER, SPAWN_S,
REGISTER_S, SPAWN_T, REGISTER_T, GUARD_PARENT, READ_NAMESPACE_BEFORE, UNSHARE,
OPEN_UID_MAP, WRITE_UID_MAP, OPEN_SETGROUPS, WRITE_SETGROUPS, OPEN_GID_MAP,
WRITE_GID_MAP, VERIFY_MAPS, VERIFY_USER_MOUNT_AFTER_UNSHARE, SET_NONDUMPABLE,
VERIFY_NONDUMPABLE, SPAWN_C, REGISTER_C, VERIFY_PID_CHILD_AFTER_REGISTER,
DUP_HIGH_FD, SET_NOFILE, INVENTORY_FDS, CLOSE_RANGE, VERIFY_HIGH_FDS,
CONTROL_CODEC, CONTROL_IO, TARGETED_REAP, FD_CLOSE, MARK_UNKNOWN, OWNER_PARK,
HISTORY_COMMIT, RELEASE_FIXTURE. No caller-supplied phase/operation.

Only direct checked refusals qualify as NATIVE_REJECTION: exact unshare flags,
fixed prctl set/get, known-owned-live pidfd_open(flags=0), and exact close_range
arguments with errno 1/13/22/38; exact proc-map open/write with errno
1/2/13/20/30/38. Other native errors, missing errno, resource pressure, EINTR or
ESRCH are operation/ownership failures. ContractError/invariant mismatch is NEVER
unsupported. A successful call followed by wrong state is INVARIANT_FAILURE;
malformed frames are PROTOCOL_FAILURE. Each refuse catch covers that single call,
not subsequent checks or an entire namespace sequence. Fault doubles are unit
evidence, not actual kernel observations.

Correct native ordering: before unshare, record T's own user/mount/PID identities.
After exact unshare and map writes, verify only changed user/mount identity,
correct maps, and T's OWN PID namespace unchanged. Do NOT read/assert a populated
pid_for_children before first C exists. Fork the fixed C, register its exact
identity in the existing R/S ledgers (and host mirror) before continuing; at
VERIFY_PID_CHILD_AFTER_REGISTER compare T's now-populated pid_for_children with
C's actual PID namespace and require C getpid()==1/getppid()==0. Never expect T
itself to migrate into that PID namespace. Linux documents both the child-only
membership change and the initially empty pid_for_children link; this is a
separate static risk in archived lines 105-115, NOT an established cause of the
old failure. [Linux PID namespace semantics](https://man7.org/linux/man-pages/man7/pid_namespaces.7.html)

Derive status after cleanup: UNKNOWN takes priority over any incomplete identity,
reap, FD, control, debt or metadata fact while retaining origin + first cleanup
failure separately. Closed internal/contract/protocol/operation failures are
FAILED. UNSUPPORTED requires a checked support refusal AND closed resources.
OBSERVED requires all four actual primitives, no failure and closed resources;
it remains native-only, never Stage 0/campaign PASS. A1 receipt v1 stays strict
and unchanged. A fixed execution v2 view has exact fields: schema literal
maestro.staging-native-execution.v2, attempt/token/generation/manifest bindings,
receipt_v1, cause|null, cleanup|null, owner_state, registered_role_bits,
reaped_role_bits, data_grant_released=false, worker_created=false. It has no
exception/release constructor. Its optional serialized diagnostic is <=1536 B
and charged to ordinary budget; no old v1 decoder silently accepts new fields.

## 3. Same-call exceptions; unchanged A1 helper semantics

NEW A2 native/owner cleanup only: primary any non-Exception BaseException wins by
exact identity; otherwise first A2 cleanup control flow wins; otherwise ordinary
primary/first ordinary cleanup. Attempt remaining eligible independent cleanup,
then propagate the selected object. Notes are fixed bounded best effort, with
no raw error/traceback; retained owners contain no exception or traceback.

Do not change A1 _close_fds or manifest helper behavior/tests: those preserve ANY
existing body primary even over a cleanup control flow. A1 is an opaque helper
call in A2. Its actually returned/raised object is the boundary outcome; A2 cannot
claim identity/propagation of a control signal A1 already suppressed internally.
A separate later A2 cleanup control signal follows NEW A2's rule, without
retroactively changing A1's internal selection. Opaque failure also creates debt
under section 1. No claim that all called helpers follow the new rule.

run_native_prerequisite must rethrow its selected local control object after
custody and bounded cleanup; it cannot swallow it into UNSUPPORTED. Process entry
catches that propagated object only at the process boundary, records finite
INTERRUPTED/UNKNOWN, drops exception references, then keeps or closes its real
owner. The host never recreates the remote exception object. A host-local control
object is separately re-raised after its own custody cleanup. No blanket
except-BaseException/os._exit can discard an open graph.

## 4. Exact control protocol, peers and budgets

H is host; edges are H<->R, R<->S, S<->T, and native-only T<->C. Each edge is a
private inherited SOCK_SEQPACKET pair with SO_PASSCRED enabled BEFORE fork/exec.
The parent retains one endpoint, child only its exact endpoint; close all
unintended inherited copies before another spawn/grant. The host control endpoint
is independent of stdout/stderr, which are captured/charged/discarded, not an
EOF completion protocol. No code/FD forwarding/path or operation selector.

Every recvmsg is bounded to 161 data bytes and CMSG_SPACE(12)=32 ancillary bytes
on this profile. Accept <=160 data bytes, no MSG_TRUNC/MSG_CTRUNC, exactly one
SOL_SOCKET/SCM_CREDENTIALS with exactly three native signed ints (pid,uid,gid).
Reject any other ancillary data, including SCM_RIGHTS; immediately own/close any
actually delivered unexpected FDs once, treating uncertainty as UNKNOWN/debt.
Use each message's credentials, not SO_PEERCRED: the latter reflects socket
creation/connection-time credentials. [Linux UNIX credentials](https://man7.org/linux/man-pages/man7/unix.7.html)

Validate pid against the already registered exact peer's namespace-visible PID,
and uid/gid against its checked mapping. H/R/S receive host U/G and registered
host-visible peer PIDs; T after mapping sees C uid/gid=0 and its parent-namespace
C PID; C sees T uid/gid=0 and PID=0 because T is outside C's PID namespace, coupled
to its inherited exact parent pidfd/parent guard. No credential-by-role-name.
For first child HELLO, parent knows the fork/Popen PID and registers its pidfd
before accepting/granting; any registration ambiguity stops progress. Identity
reports about grandchildren must come from the authenticated known parent path,
match recorded parent edges and trigger independent receiver pidfd opens before
an ACK; a supplied token or role alone never proves reap/identity.

Frame header is EXACTLY 90 B: magic SNF2 (4), version=2 (u8), opcode/src/dst
(u8 each), sequence(u16), attempt/token/generation (16 B each), manifest(32 B).
Payload is <=70 B, fixed struct by opcode, unused bytes forbidden. Codes are
finite numeric enums from this document; integers use network byte order.
Validate bound tuple, expected state/edge/opcode and next sequence before action;
sequence starts at 0 per direction, no duplicates/gaps/replay. Max 8 accepted
frames per direction. No sender-selected deadlines or FD numbers on the wire;
fixed inherited FD positions are installed by the trusted loader.

| Opcode | Exact payload / allowed edge and state |
| --- | --- |
| START | Six fixed inherited FD indices(u32 each), started/deadline(u64 each), U/G(u32 each); H->R only bootstrap. |
| GROUPS | count(u8<=16), that many u32; H->R only after START. |
| HELLO / ACK_REGISTER | subject role(u8), PID(u32), parent role(u8); child->parent then parent->child for the exact pending registration; ACK never releases a slot. |
| IDENTITY | subject role,parent(u8 each), PID/U/G(u32 each); authenticated parent reports its own fixed child; receiver opens independent pidfd then ACKs. |
| NS_IDENTITY | three (dev,ino) u64 pairs, observed C pid/ppid(u32 each); T/C edge after actual C registration only. |
| GO_NATIVE | empty; EXACT native-only H->R->S->T route, once on each edge after ACK_REGISTER(T), before unshare/C progress. Each receiver checks prefix registration/state/deadline and forwards once; fixture rejects it. |
| GO_CLOSE_RANGE | empty; native-only T->C once, after full C registration and T has verified C NS_IDENTITY at VERIFY_PID_CHILD_AFTER_REGISTER. Only then may C run close_range. |
| RESULT_PARKED / RESULT_CLOSED | status/reason/primitives/state/registered/reaped bits (six u8), cause+cleanup (each role/phase/kind u8, errno u16; 0=null); R->H once; RESULT_CLOSED needs real internal closure. |
| CONVERGE_PREUNSHARE | empty; fixture-only H->R->S->T path, once, only PARKED_UNKNOWN before shared cleanup_end. |
| EXIT_NOTE / REAP_ACK | subject role,parent(u8 each), PID(u32), exit-kind(u8), status(i32), closed-FD bitset(u64), cause+cleanup (same two fixed 5B records); exact child exit-tail/direct-parent reap edge only. Forward an unchanged child cause only on its authenticated parent chain. One explicit S->R relay of REAP_ACK(C,parent=T) is allowed after S authenticates T's original ACK and independently observes C exit; no other relay subject/edge is allowed. |
| PREFIX_CLOSED | S/T/C registered/reaped/closed bitsets(u8 each), cause+cleanup as above; R->H final, only after ledger checks. No arbitrary closed Boolean. |

### Complete native grant trace

These are the complete ordinary frames, in causal order. Sequence numbers are
PER DIRECTION and continue into terminal traffic. A parent registers its known
fork/Popen child pidfd before accepting HELLO. IDENTITY receivers independently
register the child and check the recorded parent edge before ACK. Missing or
failed steps stop; implementation cannot substitute another implicit grant.

| # | Edge and sequence | Frame / required state transition |
| --- | --- | --- |
| 1 | H->R 0 | START; fixed bootstrap binding only. |
| 2 | H->R 1 | GROUPS; completes bootstrap, no descendant grant. |
| 3 | R->H 0 | HELLO(R); H already holds R exact pidfd. |
| 4 | H->R 2 | ACK_REGISTER(R); R may establish subreaper and create its one S, no namespace progress. |
| 5 | S->R 0 | HELLO(S); R opens/registers exact S before acceptance. |
| 6 | R->H 1 | IDENTITY(S,parent=R); H registers its independent S pidfd. |
| 7 | H->R 3 | ACK_REGISTER(S); confirms H registration. |
| 8 | R->S 0 | ACK_REGISTER(S); S may create its one T, not unshare/C. |
| 9 | T->S 0 | HELLO(T); S registers exact T. |
| 10 | S->R 1 | IDENTITY(T,parent=S); R registers exact T. |
| 11 | R->H 2 | IDENTITY(T,parent=S); H registers exact T. |
| 12 | H->R 4 | ACK_REGISTER(T); completes H's R/S/T prefix registration. |
| 13 | R->S 1 | ACK_REGISTER(T); records upstream registration. |
| 14 | S->T 0 | ACK_REGISTER(T); REGISTRATION ONLY: T remains PRE_UNSHARE. |
| 15 | H->R 5 | GO_NATIVE; native family, complete prefix, no failure/parking and original deadline still live. |
| 16 | R->S 2 | GO_NATIVE; only after forwarding step 13 and verifying the same conditions. |
| 17 | S->T 1 | GO_NATIVE; only after forwarding step 14. T now has the sole authority for its fixed namespace/map sequence and, on success, ONE C fork. No data/W grant. |
| 18 | C->T 0 | HELLO(C); T has created/registered exact C, C otherwise waits. |
| 19 | T->S 1 | IDENTITY(C,parent=T); S registers exact C. |
| 20 | S->R 2 | IDENTITY(C,parent=T); R registers exact C. |
| 21 | R->H 3 | IDENTITY(C,parent=T); H registers exact C. |
| 22 | H->R 6 | ACK_REGISTER(C); H registration confirmed. |
| 23 | R->S 3 | ACK_REGISTER(C); R/H registration confirmed. |
| 24 | S->T 2 | ACK_REGISTER(C); S/R/H registration confirmed. |
| 25 | T->C 0 | ACK_REGISTER(C); C may perform only fixed own-map/PID1/namespace checks and report identity; not close_range yet. |
| 26 | C->T 1 | NS_IDENTITY; C remains alive waiting. T now checks its populated pid_for_children against registered C and T's own PID namespace unchanged. |
| 27 | T->C 1 | GO_CLOSE_RANGE; only after step 26 validates. C may use its preassigned terminal credit, test close_range/high FDs and finish. |

The ordered seqpacket edge guarantees ACK_REGISTER(T) reaches T before the
following GO_NATIVE. No extra ACK is invented. GO_NATIVE is forwarded explicitly,
not treated as an implied permission in registration ACKs. GO_CLOSE_RANGE makes
the final C check ordering explicit without allowing C to exit before T verifies
its child PID namespace. Each grant refuses outside its exact family/state/peer/
sequence or after failure/parking/deadline; never reissue a grant on retry.

Normal full-C native terminal family N_C (slots below); closed abort families R_0/R_1/S_0/T_0 and uncertain family U are explicitly defined afterward. They substitute for this family, never add another eight-credit allowance:

| Slot | Edge and next sequence | Condition / effect |
| --- | --- | --- |
| 0 | C->T 2 EXIT_NOTE(C) | C's fixed exit tail; T still independently observes/reaps exact C. |
| 1 | T->S 2 REAP_ACK(C,parent=T) | Only after T's actual targeted C reap; T then closes its exit tail and exits. |
| 2 | S->R 3 REAP_ACK(C,parent=T) | Exact one-time relay after S authenticates T's ACK and observes its own C pidfd exit; unchanged child fields. R checks its independent C pidfd and recorded C->T->S chain. |
| 3 | S->R 4 REAP_ACK(T,parent=S) | Only after S actually targeted-reaps T; S then exits. |
| 4 | R->H 4 RESULT_CLOSED (or early RESULT_PARKED on failure) | R itself observes/reaps S; the result preserves origin/cleanup classification. Never emit both alternatives. |
| 5 | R->H 5 PREFIX_CLOSED | Only real complete prefix/FD closure; host still independently observes/reaps R. |
| 6,7 | Unused terminal credits | Send NO extra frame/opcode. A failure uses the result alternative before it is sent, or stays retained UNKNOWN if the result was already sent; no repeated RESULT. |

### Finite native abort substitutions

A closed-abort family is selected from the actual created-role ledger and last
completed ordinary prefix, not a caller mode. It requires full registration/ACK
for every created descendant, no ambiguous spawn or deeper role, exact exits and
trusted targeted reaps, confirmed local FD closure and a valid bound channel.
A failed checked fork that guarantees no child may establish absence; a constructor
which did not return, failed pidfd registration or an uncertain fork may not.
An EXIT_NOTE is only a claimed exit-tail/cause; actual pidfd exit + trusted reap
still determines closure. Never infer a failed operation/errno from wait status.

| Family / last possible ordinary prefix | Exact terminal substitution in slots 0..7 | Required progression and maximum |
| --- | --- | --- |
| R_0: R has validated START/GROUPS and inherited binding, but has not sent HELLO (after step2, before step3); no S was created/attempted ambiguously | slot0 R->H seq0 RESULT_CLOSED; slot1 R->H seq1 PREFIX_CLOSED; 2..7 UNUSED | H already registered its exact R before START. H's WAIT_R_HELLO state explicitly accepts this bound early terminal alternative. R must really have no descendant, no debt and confirmed local closure, then H still observes/reaps R. Two frames; no synthetic HELLO/ACK. |
| R_1: HELLO(R) sent; R fails before any S exists (after step3 or step4, before step5) | slot0 R->H seq1 RESULT_CLOSED; slot1 R->H seq2 PREFIX_CLOSED; 2..7 UNUSED | R cause is already local: e.g. checked subreaper rejection after ACK_REGISTER(R). ACK_REGISTER(R), if already queued, creates no evidence that S exists. No new S may start after the failure. Two frames. |
| S_0: S fully registered/ACKed through step8; S fails before T exists | slot0 S->R seq1 EXIT_NOTE(S,parent=R); slot1 R->H seq2 RESULT_CLOSED; slot2 R->H seq3 PREFIX_CLOSED; 3..7 UNUSED | S authenticates its own exact finite cause and closes its exit tail; R independently observes/targeted-reaps S before RESULT_CLOSED. R carries the unchanged S cause; no fictional T message or S-reap ACK to itself. Three frames. |
| T_0: T fully registered through step14, no C exists; failure while parked or after GO_NATIVE steps15-17 | slot0 T->S seq1 EXIT_NOTE(T,parent=S); slot1 S->R seq2 REAP_ACK(T,parent=S); slot2 R->H seq3 RESULT_CLOSED; slot3 R->H seq4 PREFIX_CLOSED; 4..7 UNUSED | S authenticates T's note, actually targeted-reaps T and carries its unchanged cause in REAP_ACK; S then exits. R independently checks T exit/report and targeted-reaps S before closed result/prefix. Four frames. Actual unshare refusal after step17 therefore preserves T's phase/kind/errno. Before receipt of step17, no unshare cause may be claimed. |

RESULT_CLOSED here means only confirmed resource closure, not operation success.
The enclosed status remains the existing classification: e.g. UNSUPPORTED only
for a real checked refusal, FAILED for a closed internal error, UNKNOWN/interrupted
for a remote control-flow event. The original failure and first cleanup record
remain finite/distinct. PREFIX_CLOSED reports the actually created set (empty,
S, or S/T); it never lists a nonexistent C. Native slots stay UNKNOWN regardless.

All these notes are sent only by a peer whose exact identity/parent/binding is
already registered. An S failure before step8 or a T failure before step14 does
NOT use a closed family. A cause from an early peer cannot bypass HELLO/identity/
ACK obligations by masquerading as a later-role note. Missing prerequisites go
to U; no implementation-chosen substitute ACK/grant/exit message is allowed.

Family U is REQUIRED for partial registration, ambiguous spawn/ACK/send state,
missing reap, incomplete FD/metadata closure or a role unable to send its legal
authenticated closed-abort note. If the H/R binding and next outbound sequence
are certain and no RESULT was sent, R may spend ONE terminal credit on
RESULT_PARKED. It uses the next R->H sequence of the actually sent prefix:

| Ordinary prefix reached, without a previous RESULT | Next R->H RESULT_PARKED sequence |
| --- | --- |
| after validated steps1-2, before step3 | 0 |
| step3 through5 | 1 |
| step6 through10 | 2 |
| step11 through20 | 3 |
| step21 through27 | 4 |

This is a counter rule, not permission to reset sequence after an uncertain send:
once any edge's transmission/delivery sequence is ambiguous, send no further
frame on that edge. H then records UNKNOWN/control_lost and retains its exact
owner/handles. Likewise R failure before START/GROUPS/binding validation has no
legal bound cause frame; H cannot recover its phase/errno from stderr/exit status.
Use UNKNOWN/control_lost with null remote cause, not an invented native refusal.

If a legal cause was already authenticated before uncertainty, retain it; otherwise
the receiving owner records only its own finite CONTROL_IO/control_lost observation
with errno null. The original remote cause may remain at the last actual owner;
do not assert it crossed an edge which could not authenticate/transport it.
No S/T/C EXIT_NOTE or REAP_ACK is fabricated for U. In particular an unconfirmed
reap cannot be forwarded as a cause-carrying REAP_ACK just to complete the trace.
R and each existing descendant holding an unresolved local ledger remain owned/
parked; U authorizes no PREFIX_CLOSED or normal R/descendant exit while that
obligation remains ambiguous. Hard/external death is reported as uncertainty,
never chosen as a successful normal-exit path.

If uncertainty occurs after an already emitted normal terminal prefix, do not
restart slots or replay a RESULT: keep earlier facts, use at most that one
RESULT_PARKED if none was sent and its sequence is certain, then retain. An earlier
RESULT_CLOSED/PREFIX_CLOSED is never enough for host success without actual R exit/
reap and all surviving FD closure. Later true independent closure follows section5;
no earlier missing evidence is supplied by the terminal label alone.

Closed R/S/T aborts use at most 2/3/4 frames; U uses zero or one new R report and
no child fallback frames. A normal terminal prefix before U has at most six
frames, and if it already includes a RESULT then U adds none. All families stay
within the SAME eight terminal credits, <=192 B each; unused credits stay unused.
Every outgoing sequence is the next existing edge counter, never a fresh abort
sequence. Closed T_0's directional maxima are H/R<=6/5, R/S<=3/3, S/T<=2/2;
S_0 is H/R<=4/4,R/S<=1/2; R_0 is H/R<=2/2 and R_1<=3/3. U cannot exceed normal
per-direction ceilings: its one possible R report occurs at sequence0..4.
Normal 27 ordinary frames and all registration/grant semantics remain unchanged.

The retention fixture takes ordinary steps 1-14 ONLY, then returns PARKED_UNKNOWN.
Its terminal order is fixed: R->H 3 RESULT_PARKED; H->R 5 CONVERGE; R->S 2
CONVERGE; S->T 1 CONVERGE; T->S 1 EXIT_NOTE(T); S->R 2 REAP_ACK(T,parent=S);
R->H 4 PREFIX_CLOSED after R's own S reap; terminal slot 7 is unused, not another opcode/result. It has
no GO_NATIVE, C or GO_CLOSE_RANGE, and keeps the same original cleanup deadline.

| Edge | Native ordinary directions | Native terminal included: max per direction | Fixture max per direction |
| --- | --- | --- | --- |
| H/R | H->R 7, R->H 4: total 11 | H->R 7, R->H 6; two terminal credits unused | H->R 6, R->H 5; one terminal credit unused |
| R/S | R->S 4, S->R 3: total 7 | R->S 4, S->R 5 | R->S 3, S->R 3 |
| S/T | S->T 3, T->S 2: total 5 | S->T 3, T->S 3 | S->T 2, T->S 2 |
| T/C | T->C 2, C->T 2: total 4 | T->C 2, C->T 3 | none |

Thus ordinary native maximum is 11+7+5+4=27 frames; fixture ordinary is 14.
Every direction stays within eight frames including the assigned terminal slots.
No unlisted ordinary message is allowed; failure handling uses only the bounded
terminal forms and never omits a required registration/grant to save credits.

This is a finite set of fixed structs, not a generic message dictionary. The
fixture has no GO_NATIVE/NS_IDENTITY/C edge. The trusted Popen launcher uses a fresh exact environment containing only LANG/LC_ALL and six fixed STAGING_*_FD keys for source,arena,root,parent,shared,control. Values come only from this Owner's recorded pass_fds, never the public request. The entry validates those inherited descriptors against their fixed roles/bindings before use; START indices are the constant role ordinals 0..5, not selectable descriptors. All descendants inherit only the exact endpoint/root subset owned for their edge.

Role identifiers do not create a
process. Trusted reap reports bind actual authenticated sender/parent edge and
attempt; receiver independently verifies its exact pidfd exit. Host R exit and
trusted targeted reap are observed by H itself.

Pre-reserve 2048 B in the shared counter before ordinary traffic. Ordinary cannot
spend it. Charge every normal frame conservatively 192 B (<=160 data + 32 B
ancillary capacity), once at producer, not again at receiver. Strict state/op
sequence counters cap accepted frames independently. Unexpected stdout/stderr
bytes charge once on capture; they are discarded, never raw diagnostics.

| Shared allowance | Hard maximum |
| --- | --- |
| Exact ordinary native trace: H/R=11, R/S=7, S/T=5, T/C=4 | 27*192=5184 B |
| New baseline artifact / optional typed v2 diagnostic | 1024 + 1536 B, ordinary |
| Other captured stderr/stdout/control failures | remaining 6592 B of ordinary 14336; saturation refuses, never borrows terminal credits |
| Terminal: cause/cleanup are fused into one RESULT; H/R/S forwarded CONVERGE; descendant EXIT/REAP; final PREFIX_CLOSED | <=8 frames *192=1536 B |
| Fixed history artifact | <=512 B from terminal reserve |
| Total terminal reserve | 1536+512=2048 B; total <=16384 B |

Terminal frame slots are assigned before sending: fixture RESULT, H CONVERGE,
R forward, S forward, T EXIT_NOTE, S REAP_ACK, R PREFIX_CLOSED, one unused credit. Native mode selects exactly one listed terminal family: normal N_C uses six frames, closed R_0/R_1/S_0/T_0 aborts use 2/2/3/4, or U uses its bounded retained-state substitution. Fixture uses its seven listed terminal frames, with one unused credit. No unused credit authorizes an unlisted message; the explicit R/S/T abort notes are substitutions defined above, not additional normal-family frames. Before C closes its shared FD in close_range, its single EXIT_NOTE credit is preassigned/charged; C may only use that fixed already-budgeted terminal frame afterward.
No extra standalone cause/cleanup/heartbeat/
ACK/history copies; failure to fit is UNKNOWN with owner retained. A frame is
charged once before send; receive validates its fixed slot/sequence. Terminal
credits are already charged, so do not charge them a second time. History may
reuse neither an ordinary allowance nor a second reserve.

### C's surviving terminal endpoint

Before GO_CLOSE_RANGE is acted on, C prepays its fixed terminal credit, retains
its current channel sequence/binding, and calls dup2(T_control_socket_fd, 1) in
its OWN process. Successful dup2 atomically closes ONLY C's old stdout capture
copy at FD1 and installs the same T/C seqpacket endpoint there; it does not close
R/S/T capture copies. Detach the old Python socket wrapper, registering its raw
old FD>=3 as part of the known close_range set, so a destructor cannot later
close a reused number. Own the FD1 socket wrapper before another fallible action;
its sequence counter is continued, not reset. Any other unintended C-local
capture/endpoint copies are already removed by the fixed inherited-FD rule.

The actual close_range(3,UINT_MAX,UNSHARE) then closes the old endpoint number and
all other known nonstdio FDs, while FD1 remains the sole C terminal endpoint.
After verifying the expected closures, C sends EXIT_NOTE through FD1's seqpacket
socket, with the same per-message credentials and assigned next sequence, then
marks/attempts that FD1 close once before actual exit. T's endpoint stays intact
and T independently observes/reaps C. Prepaid bytes alone never imply a usable
socket. Dup2/detach/wrapper/close uncertainty retains UNKNOWN; no guessed old FD,
new endpoint, extra frame or normal C exit is authorized to repair the channel.

## 5. Park, late convergence and actual exit

The host retains its strong Owner with R pidfd/control/refs; R's process-root
RunnerOwner retains the full actual S/T/C ledger before any spawn. On first
failure/terminal decision set shared cleanup_end ONCE to
min(now+5s, original_deadline+5s). New cleanup never refreshes it.

R transitions RUNNING -> CLEANING -> PARKED_UNKNOWN if closure is incomplete.
RESULT_PARKED returns the host call early without EOF; entry continues holding
RunnerOwner. The real retention fixture deliberately reaches this state before
unshare, with registered T still alive, while the original cleanup window has
remaining time. H verifies R alive + its strong handles, then sends the one fixed
CONVERGE. R/S/T each check authenticated family/state/sequence and monotonic time
<cleanup_end BEFORE forwarding/obeying. T then exits without unshare/C; S/R only
perform the targeted current-prefix reaps/closes. No ACK starts another window.

After cleanup_end or peer loss, reject CONVERGE and never forward it. Native
mode never accepts it. EOF/lost sender does not authorize R exit. A parked owner
then only passively polls existing pidfds and does targeted WNOHANG reaps/once-
only closes when readiness permits: no new signal, helper, namespace operation,
probe grant, output stream or cleanup allowance. Retain finite state indefinitely
if convergence cannot be confirmed. Passive later closure can permit actual R
exit, not another active cleanup attempt or automatic slot release.

PREFIX_CLOSED requires complete created-role registration, exact exit + trusted
targeted reaps, no ambiguous spawn, and confirmed descendant/nonterminal FD
closure. Only then close the exact R exit-tail control/shared resources and exit.
H independently observes/reaps R and closes its attempt FDs. A close uncertainty
retains owner/debt, not normal exit based on R main returning. RESULT/PREFIX
frames are observations, not caller-decodable release capabilities. Hard R/host
failure remains outside a proven retained-owner lifetime; no fake normal closure.

## 6. Existing root, new raw baseline and durable local history

Add owner-aware `open_fixture_roots_existing` for the FIXED preserved parent
/tmp/maestro-staging-a2-native-20261115. Use no-follow component/dirfd opens,
EXISTING fixed names and A1 owner/mode/bounded-name/header/identity/digest/revision/
empty-anchor checks. No mkdir, create-root, truncate, repair, cleanup or fallback.
Independently open lock descriptions. Every returned FD is recorded immediately;
opaque A1 failure follows debt/old helper semantics in sections 1/3.

BEFORE any reservation/new metadata mutation, read exact slot-0.record raw 512 B
and current validated header entry[0] digest/revision, generation and all binding
identities under A1's lock. Require observed slot0 UNKNOWN and slot1 FREE. This is
a NEW OBSERVED baseline, NOT historical bytes reconstructed from the old JSON.
Store it separately in fixed `staging-a2-baseline-v2.bin` (1024 B strict zero-padded
binary codec: magic/version, raw512, header0 digest32/revisionu64, generation16,
nine (dev,ino) u64 pairs, remainder zero). It fits <=1024 and is ordinary-charged.

Compare raw512, header digest/revision[0], generation and binding identities after
reopen, reserve, mark UNKNOWN, release-intent and final release readback, including
every failure path before any next mutation. At release-intent, compare the raw slot0 and explicitly parsed header[0] while pending is expected; do not call A1's committed-only _read on an in-flight intent or weaken its pending refusal. Header fields [1] may advance; [0]
and slot0 bytes never do. A mismatch poisons/refuses; no repair/history inference.
This proves preservation only SINCE this new baseline. Coherent rollback or a
replacement of the complete state is not independently detected by A1.

Canonical audit location is the SAME already-owned Linux parent directory, not
the Windows worktree ledger. Require fstatfs magic 0xEF53 AND the anchored mount-ID
mapping to fs type ext4; bounded mountinfo parsing must agree. Any 9p/drvfs,
unknown mapping, unsupported directory fsync or uncertain persistence refuses
release; do not choose another arena/location. No claim of arbitrary physical
storage/power-loss durability beyond the supported ext4 fsync contract.

History leaf is `staging-a2-retention-v2.bin`, fixed 512 B strict padded binary:
magic/version, attempt/token/generation, manifest digest, baseline SHA256, fixed
origin UNKNOWN event, exact R/S/T PID/parent/exit/reap facts and final local-FD/
closure bitset; reserved bytes zero. No path/raw message/exception payload. Its
original UNKNOWN and final closure are distinct fields, never overwritten into
success. It is an AUDIT precondition, not release authority or rollback protection.

For BOTH fixed artifacts: anchored parent_fd, O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC
0600; verify regular/nlink1/owner and exact size; perform one full bounded write,
fsync(file), once-only close, fsync(parent directory), then strict readback via a
new owner-recorded FD and close. Short write, close/fsync/readback error, existing
unexpected leaf or unknown filesystem => poison/no release; no unlink/retry/
replace. The first historical baseline is never recomputed to make a mismatch
pass. A later optional ledger mirror has no release role and must be charged
within remaining ordinary budget; do not rely on its 9p durability.

Root's eight administration FDs are the explicit exception to attempt-FD closure;
the parent administration FD persists the history, and the owned SlotArena FDs
perform release after all attempt handles close. Neither was a data/ns reference.
No A1 schema migration: v1 field sets/sizes stay; FREE at a higher revision is
already valid. Slot0 bytes/header[0] stay unchanged; no historical dump is claimed.

## 7. Exact new fixture release and later capacity

A separate fixed private retention-fixture entry/request domain creates only
R/S/T. Public NativePrerequisiteRequest has no CaseId/fault selector and rejects
that request type. No code/FD/PID operation input. The manifest-bound fixture T
has NO unshare/C/data/Worker path and no grant issued for one. Its real early
UNKNOWN is a custody observation, never native OBSERVED/capability evidence.

Only this new current execution can privately mint ClosureEvidence: continuous
root/owner without debt; dual exact R/S/T registration and parent edges; authenticated
T parked/exit and S/R targeted-reap reports; independent H pidfd exits; actual
H targeted reap of R; every attempt FD closed; original grant ledger empty;
raw baseline unchanged; durable fixed history committed. Host must not rely only
on R's self-described closure. Private evidence has no constructor/from_bytes,
caller bool or receipt reconstruction; root registers its exact object identity,
reservation token/index/generation and fixture family, consumes it once under
its lock, then performs the existing A1 intent/record/header fsync order outside
the lock. Any write ambiguity leaves poisoned/unavailable state, no retry/reset.

Release advances ONLY the new slot1 to FREE with a higher revision/same generation;
it preserves its earlier UNKNOWN history. Original slot0 can NEVER mint evidence
from old receipts/reports and remains held. If any fixture fact fails, both slots
remain unavailable; do not create a fresh root or issue a forged release.

After a proved fixture release, a separately scheduled/reviewed native acceptance
attempt may reserve slot1 in that SAME continuously bound root. It STILL has no
release rule: even native OBSERVED with actual closure leaves slot1 UNKNOWN, while
slot0 stays held. Thus A2 success supplies NO capacity/permission for B/C. B/C need
a concrete separately reviewed consumer/release design for the original complete
R/S/T/C pre-data closure rule; do not choose or implement it implicitly here.

## Scoped REDs, increments and review focus

Future REDs (not executed): reviewer ContractError reproductions become FAILED,
phase/errno never fabricated; successful unshare plus legal empty pre-child link
must not read/reject pid_for_children, T pid stays original, C registration then
PID1 validation occurs at VERIFY_PID_CHILD_AFTER_REGISTER. Real support denial
and internal mismatch remain distinct; cleanup unknown wins without losing cause.

Local custom BaseException identity survives NEW A2 cleanup; accepted A1
RuntimeError/SystemExit priority tests remain unchanged. Before-token helper/
close failure retains poisoned debt and refuses admission. Alias/reload/fork/
subinterpreter/other-thread/reentrant entry refuses; root.close cannot lose owner.

Protocol REDs cover socket creation credentials differing from exec sender,
missing/wrong/extra SCM_CREDENTIALS, rights/truncation, wrong peer/role/edge/seq/
attempt, false reap and credit overflow. Test the table's worst-case bytes,
no EOF wait, late/lost CONVERGE, no refreshed deadline and parked peer-loss survival.

Real fixed PRE_UNSHARE test proves R alive/strong root and actual registered S/T
ledger after early return, then exact convergence/reaps inside the original
window. No namespace success or C is needed. Read-only reopen + new raw baseline
prove no slot0/header0 change at each transition. 9p/directory-fsync/history failure,
missing host reap/FD closure, stale/decoded evidence and release replay all refuse.

Finite later increments: typed failures/PID-order/A1 boundary + strict baseline
reopen; concrete retained owners/protocol/accounting; real pre-unshare custody
fixture/history/one-shot release. Independent scoped design review is required
before any source restoration/development, and no second native acceptance is
authorized now. Review these exact decisions, not a promise of a future general
owner framework. Native failure/root-cause, lifetime closure and storage support
remain empirical requirements; unsupported/unknown never becomes fake PASS or
complete Stage0/P3.
