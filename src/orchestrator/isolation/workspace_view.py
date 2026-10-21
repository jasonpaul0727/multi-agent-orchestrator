"""Bounded, no-follow inventories of stopped final workspace views.

An absent path is represented by a baseline-bound tombstone, never a filesystem
whiteout. Candidate storage contains only changed payloads and parent scaffolds.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat

from .workspace import (
    WorkspaceBoundaryError, WorkspaceConflict, WorkspaceConflictReport,
    WorkspaceDiff, WorkspaceDiffEntry, _descriptor_mount_id, _is_within,
    _manifest_digest, _reject_xattrs, _validate_relative_symlink, _workspace_diff_hash,
)


def _digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


_EMPTY = _digest(b"")


@dataclass(frozen=True)
class _Item:
    kind: str
    mode: int
    size: int
    digest: str
    payload: bytes = b""


def _bounds(max_entries, max_bytes, max_depth):
    for name, value, minimum in (("max_entries", max_entries, 1), ("max_bytes", max_bytes, 0), ("max_depth", max_depth, 1)):
        if type(value) is not int or value < minimum:
            raise WorkspaceBoundaryError(f"{name} must be a valid bound")


def _path(path: str, max_depth: int) -> tuple[str, ...]:
    if not isinstance(path, str):
        raise WorkspaceBoundaryError("invalid workspace path")
    parts = tuple(path.split("/"))
    if (len(parts) > max_depth or any(p in ("", ".", "..") or "\\" in p
            or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in p) for p in parts)
            or parts[0].casefold() in (".git", ".maestro", ".env")):
        raise WorkspaceBoundaryError("invalid or protected workspace path")
    return parts


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def _names(fd: int, limit: int) -> list[str]:
    names = []
    with os.scandir(fd) as entries:
        for entry in entries:
            names.append(entry.name)
            if len(names) > limit:
                raise WorkspaceBoundaryError("workspace exceeds entry limit")
    return sorted(names)


def _validate_symlink_chains(items: dict[str, _Item], max_depth: int) -> None:
    """Resolve only descriptor-inventoried entries, never host-follow a link.

    Expansion precedes ``..`` processing, matching filesystem resolution. The
    same resolver can inspect a complete view or a baseline/payload composition
    without materializing that composition or opening any external destination.
    Missing paths may remain dangling, but cannot hide a lexical root escape.
    """
    for path, item in items.items():
        if item.kind != "symlink":
            continue
        pending = deque(path.split("/"))
        resolved: list[str] = []
        links = 0
        steps = 0
        while pending:
            steps += 1
            if steps > 41 * (max_depth + 1):
                raise WorkspaceBoundaryError("symlink resolution exceeds its path bound")
            component = pending.popleft()
            if component in ("", "."):
                continue
            if component == "..":
                if not resolved:
                    raise WorkspaceBoundaryError("symlink chain escapes its workspace root")
                resolved.pop()
                continue
            current = "/".join((*resolved, component))
            _path(current, max_depth)
            target = items.get(current)
            if target is not None and target.kind == "symlink":
                links += 1
                if links > 40:
                    raise WorkspaceBoundaryError("symlink chain exceeds the resolution limit")
                pending.extendleft(reversed(os.fsdecode(target.payload).split("/")))
            else:
                if target is not None and target.kind != "directory" and pending:
                    raise WorkspaceBoundaryError("symlink chain traverses a non-directory")
                resolved.append(component)


def _inventory(root: Path, *, max_entries: int, max_bytes: int, max_depth: int,
               payloads: bool = False, private: bool = False, live: bool = False) -> dict[str, _Item]:
    """Read through held directory handles, checking identity before/after reads."""
    items: dict[str, _Item] = {}
    identities: dict[str, tuple] = {}
    children: dict[tuple[str, ...], list[str]] = {}
    total = 0
    try:
        initial = root.lstat()
        if not stat.S_ISDIR(initial.st_mode):
            raise WorkspaceBoundaryError("inventory root must be a real directory")
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            if _signature(os.fstat(fd)) != _signature(initial):
                raise WorkspaceBoundaryError("inventory root changed while opening")
            mount = _descriptor_mount_id(fd)

            def walk(parent: int, relative: tuple[str, ...]):
                nonlocal total
                start = os.fstat(parent)
                if private and stat.S_IMODE(start.st_mode) != 0o700:
                    raise WorkspaceBoundaryError("candidate directory permissions are not private")
                _reject_xattrs(parent)
                names = _names(parent, max_entries)
                children[relative] = names
                folded: set[str] = set()
                for name in names:
                    if live and not relative and name.casefold() in (".git", ".maestro", ".env"):
                        continue
                    path = "/".join((*relative, name))
                    _path(path, max_depth)
                    if name.casefold() in folded:
                        raise WorkspaceBoundaryError("workspace has case-colliding paths")
                    folded.add(name.casefold())
                    if len(items) >= max_entries:
                        raise WorkspaceBoundaryError("workspace exceeds entry limit")
                    before = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    identities[path] = _signature(before)
                    mode = stat.S_IMODE(before.st_mode) & 0o777
                    if stat.S_ISLNK(before.st_mode):
                        link_fd = os.open(name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
                        try:
                            if _signature(os.fstat(link_fd)) != _signature(before) or _descriptor_mount_id(link_fd) != mount:
                                raise WorkspaceBoundaryError("symlink changed or crosses a nested mount")
                        finally:
                            os.close(link_fd)
                        if before.st_nlink != 1:
                            raise WorkspaceBoundaryError("workspace contains a hard-linked symlink")
                        link = os.readlink(name, dir_fd=parent)
                        if "\\" in link or any(ord(c) < 32 or ord(c) == 127 for c in link):
                            raise WorkspaceBoundaryError("invalid symlink target")
                        _validate_relative_symlink(relative, link)
                        resolved = list(relative)
                        for part in link.split("/"):
                            if part == "..": resolved.pop()
                            elif part not in ("", "."): resolved.append(part)
                        if resolved and resolved[0].casefold() in (".git", ".maestro", ".env"):
                            raise WorkspaceBoundaryError("symlink targets a protected path")
                        if os.listxattr(f"/proc/self/fd/{parent}/{name}", follow_symlinks=False):
                            raise WorkspaceBoundaryError("symlink contains extended attributes")
                        data = os.fsencode(link)
                        total += len(data)
                        # Targets are needed even for unchanged baseline links:
                        # another changed link can alter their composed meaning.
                        item = _Item("symlink", 0o777, len(data), _digest(data), data)
                    elif stat.S_ISDIR(before.st_mode) or stat.S_ISREG(before.st_mode):
                        directory = stat.S_ISDIR(before.st_mode)
                        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
                        child = os.open(name, flags | (os.O_DIRECTORY if directory else 0), dir_fd=parent)
                        try:
                            if _signature(os.fstat(child)) != _signature(before):
                                raise WorkspaceBoundaryError("workspace entry changed while opening")
                            if _descriptor_mount_id(child) != mount:
                                raise WorkspaceBoundaryError("workspace contains a nested mount")
                            _reject_xattrs(child)
                            if directory:
                                item = _Item("directory", mode, 0, _manifest_digest("directory", mode, 0))
                                items[path] = item
                                walk(child, (*relative, name))
                            else:
                                if before.st_nlink != 1:
                                    raise WorkspaceBoundaryError("workspace contains a hard-linked file")
                                if private and mode != 0o600:
                                    raise WorkspaceBoundaryError("candidate file permissions are not private")
                                if before.st_size > max_bytes - total:
                                    raise WorkspaceBoundaryError("workspace exceeds byte limit")
                                chunks = []
                                digest = hashlib.sha256()
                                size = 0
                                while chunk := os.read(child, min(1024 * 1024, max_bytes - total + 1)):
                                    total += len(chunk)
                                    size += len(chunk)
                                    if total > max_bytes:
                                        raise WorkspaceBoundaryError("workspace exceeds byte limit")
                                    digest.update(chunk)
                                    if payloads: chunks.append(chunk)
                                if size != before.st_size:
                                    raise WorkspaceBoundaryError("workspace changed during inventory")
                                item = _Item("file", mode, size, "sha256:" + digest.hexdigest(), b"".join(chunks))
                            if _signature(os.fstat(child)) != _signature(before):
                                raise WorkspaceBoundaryError("workspace changed during inventory")
                        finally:
                            os.close(child)
                    else:
                        raise WorkspaceBoundaryError("workspace contains a whiteout or special file")
                    if total > max_bytes:
                        raise WorkspaceBoundaryError("workspace exceeds byte limit")
                    if _signature(os.stat(name, dir_fd=parent, follow_symlinks=False)) != _signature(before):
                        raise WorkspaceBoundaryError("workspace changed during inventory")
                    items[path] = item
                if _names(parent, max_entries) != names or _signature(os.fstat(parent)) != _signature(start):
                    raise WorkspaceBoundaryError("workspace directory changed during inventory")

            def verify(parent: int, relative: tuple[str, ...]):
                names = _names(parent, max_entries)
                if names != children[relative]:
                    raise WorkspaceBoundaryError("workspace changed during inventory")
                for name in names:
                    if live and not relative and name.casefold() in (".git", ".maestro", ".env"):
                        continue
                    parts = (*relative, name)
                    path = "/".join(parts)
                    directory = items[path].kind == "directory"
                    flags = (os.O_RDONLY | os.O_DIRECTORY) if directory else os.O_PATH
                    child = os.open(name, flags | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
                    try:
                        if _signature(os.fstat(child)) != identities[path] or _descriptor_mount_id(child) != mount:
                            raise WorkspaceBoundaryError("workspace changed during inventory")
                        if directory:
                            verify(child, parts)
                        if _signature(os.stat(name, dir_fd=parent, follow_symlinks=False)) != identities[path]:
                            raise WorkspaceBoundaryError("workspace changed during inventory")
                    finally:
                        os.close(child)

            walk(fd, ())
            _validate_symlink_chains(items, max_depth)
            verify(fd, ())
            if _signature(root.lstat()) != _signature(initial):
                raise WorkspaceBoundaryError("inventory root changed during inspection")
        finally:
            os.close(fd)
    except (OSError, ValueError, RuntimeError) as exc:
        if isinstance(exc, WorkspaceBoundaryError): raise
        raise WorkspaceBoundaryError("workspace inventory cannot be read safely") from exc
    return items


def _preimages(items: dict[str, _Item]) -> dict[str, str]:
    """Merkle directory preimages bind sorted names, types, modes and contents.

    Each descendant is included through its parent digest, avoiding quadratic
    rescans of wide trees. Snapshot write-bit normalization is not a live backup.
    """
    def snapshot_mode(item: _Item) -> int:
        if item.kind == "symlink":
            return 0o777
        return 0o555 if item.kind == "directory" or item.mode & 0o111 else 0o444

    children: dict[str, list[str]] = {}
    for path in items:
        parent = path.rpartition("/")[0]
        children.setdefault(parent, []).append(path)
    digests = {}
    for path in sorted(items, key=lambda p: (-p.count("/"), p)):
        item = items[path]
        if item.kind != "directory":
            digests[path] = item.digest
            continue
        records = [(child.rpartition("/")[2], items[child].kind, snapshot_mode(items[child]),
                    items[child].size, digests[child]) for child in sorted(children.get(path, []))]
        canonical = json.dumps([snapshot_mode(item), records], separators=(",", ":")).encode()
        digests[path] = _digest(b"maestro.workspace-directory.v2\0" + canonical)
    return digests


def _nonoverlap(*roots: Path):
    try:
        resolved = [root.resolve(strict=False) for root in roots]
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceBoundaryError("workspace roots cannot be resolved") from exc
    for i, root in enumerate(resolved):
        for other in resolved[i + 1:]:
            if _is_within(root, other) or _is_within(other, root):
                raise WorkspaceBoundaryError("workspace roots overlap")


def _root_identities(*roots: Path):
    try:
        return tuple(_signature(root.lstat()) for root in roots)
    except OSError as exc:
        raise WorkspaceBoundaryError("workspace roots cannot be inspected") from exc


def export_workspace_view_diff(lower_root: str | Path, view_root: str | Path, candidate_root: str | Path,
                               *, max_entries: int = 100_000, max_bytes: int = 1_073_741_824,
                               max_depth: int = 256) -> WorkspaceDiff:
    """Export changed payloads and every removed descendant from a stopped view."""
    _bounds(max_entries, max_bytes, max_depth)
    lower, view, candidate = map(Path, (lower_root, view_root, candidate_root))
    _nonoverlap(lower, view, candidate)
    if candidate.exists() or candidate.is_symlink():
        raise WorkspaceBoundaryError("candidate destination must not already exist")
    bounds = dict(max_entries=max_entries, max_bytes=max_bytes, max_depth=max_depth)
    roots_before = _root_identities(lower, view)
    baseline = _inventory(lower, **bounds)
    preimages = _preimages(baseline)
    final = _inventory(view, **bounds, payloads=True)
    if _root_identities(lower, view) != roots_before:
        raise WorkspaceBoundaryError("workspace roots changed during export")
    names = sorted(baseline.keys() | final.keys())
    if len({name.casefold() for name in names}) != len(names):
        raise WorkspaceBoundaryError("workspace has case-colliding paths")
    entries = []
    for name in names:
        before, after = baseline.get(name), final.get(name)
        if before and after and before.kind != after.kind:
            raise WorkspaceBoundaryError("workspace changes a path's file type")
        if before and after and (before.kind, before.mode, before.size, before.digest) == (after.kind, after.mode, after.size, after.digest):
            continue
        if after is None:
            entries.append(WorkspaceDiffEntry(name, "delete", before.kind, 0, 0, _EMPTY, preimages[name]))
        else:
            entries.append(WorkspaceDiffEntry(name, "modify" if before else "add", after.kind, after.mode,
                                             after.size, after.digest, preimages[name] if before else None))
    if len(entries) > max_entries:
        raise WorkspaceBoundaryError("manifest exceeds entry limit")
    candidate.mkdir(mode=0o700)
    try:
        for entry in entries:
            if entry.operation == "delete": continue
            target = candidate / entry.path
            parent = candidate
            for component in entry.path.split("/")[:-1]:
                parent /= component
                parent.mkdir(mode=0o700, exist_ok=True)
            if entry.kind == "directory": target.mkdir(mode=0o700, exist_ok=True)
            elif entry.kind == "symlink": target.symlink_to(os.fsdecode(final[entry.path].payload))
            else:
                with target.open("xb") as output: output.write(final[entry.path].payload)
                target.chmod(0o600)
        diff = WorkspaceDiff(candidate, tuple(entries), sum(e.size for e in entries),
                             _workspace_diff_hash(entries, schema_version=2), 2)
        validate_workspace_view_candidate(lower, diff, **bounds)
        if _root_identities(lower, view) != roots_before:
            raise WorkspaceBoundaryError("workspace roots changed during export")
        return diff
    except BaseException:
        shutil.rmtree(candidate)
        raise


def validate_workspace_view_candidate(lower_root, diff, *, max_entries=100_000, max_bytes=1_073_741_824, max_depth=256):
    """Independently bind payloads and complete tombstones to the frozen baseline."""
    _bounds(max_entries, max_bytes, max_depth)
    if (type(diff.total_bytes) is not int or not 0 <= diff.total_bytes <= max_bytes
            or not isinstance(diff.entries, tuple) or len(diff.entries) > max_entries):
        raise WorkspaceBoundaryError("invalid candidate manifest bounds")
    if not isinstance(diff.candidate_root, (str, Path)):
        raise WorkspaceBoundaryError("invalid candidate root")
    lower, candidate = Path(lower_root), Path(diff.candidate_root)
    _nonoverlap(lower, candidate)
    bounds = dict(max_entries=max_entries, max_bytes=max_bytes, max_depth=max_depth)
    roots_before = _root_identities(lower, candidate)
    baseline = _inventory(lower, **bounds)
    preimages = _preimages(baseline)
    payload = _inventory(candidate, **bounds, private=True)
    declared = {}
    total = 0
    for entry in diff.entries:
        if not isinstance(entry, WorkspaceDiffEntry):
            raise WorkspaceBoundaryError("invalid candidate manifest entry")
        _path(entry.path, max_depth)
        if (entry.path in declared or entry.kind not in ("file", "directory", "symlink")
                or type(entry.mode) is not int or not 0 <= entry.mode <= 0o777
                or type(entry.size) is not int or entry.size < 0
                or not isinstance(entry.digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", entry.digest)):
            raise WorkspaceBoundaryError("invalid candidate manifest values")
        before, actual = baseline.get(entry.path), payload.get(entry.path)
        if entry.operation == "add":
            if before is not None or entry.baseline_digest is not None:
                raise WorkspaceBoundaryError("added entry has a baseline")
        elif entry.operation in ("modify", "delete"):
            if before is None or before.kind != entry.kind or entry.baseline_digest != preimages[entry.path]:
                raise WorkspaceBoundaryError("candidate baseline does not match")
        else:
            raise WorkspaceBoundaryError("unsupported candidate operation")
        if entry.operation == "delete":
            if actual is not None or (entry.mode, entry.size, entry.digest) != (0, 0, _EMPTY):
                raise WorkspaceBoundaryError("invalid deletion payload or metadata")
        else:
            expected_digest = _manifest_digest("directory", entry.mode, 0) if entry.kind == "directory" else (actual.digest if actual else None)
            if (actual is None or actual.kind != entry.kind or actual.size != entry.size or expected_digest != entry.digest
                    or (entry.kind == "symlink" and entry.mode != 0o777)):
                raise WorkspaceBoundaryError("candidate payload does not match manifest")
        declared[entry.path] = entry
        total += entry.size
    if list(declared) != sorted(declared) or len({p.casefold() for p in baseline.keys() | declared.keys()}) != len(baseline.keys() | declared.keys()):
        raise WorkspaceBoundaryError("manifest paths are unsorted or case-colliding")
    deleted_directories = {path for path, entry in declared.items() if entry.operation == "delete" and entry.kind == "directory"}
    for child in baseline:
        if any(str(parent) in deleted_directories for parent in Path(child).parents):
            if child not in declared or declared[child].operation != "delete":
                raise WorkspaceBoundaryError("directory removal omits a descendant")
    scaffolds = {parent for path, entry in declared.items() if entry.operation != "delete"
                 for parent in (str(p) for p in Path(path).parents) if parent != "."}
    for path in payload.keys() - declared.keys():
        if path not in scaffolds or path not in baseline or baseline[path].kind != "directory" or payload[path].kind != "directory":
            raise WorkspaceBoundaryError("candidate contains undeclared payload")
    if total != diff.total_bytes or diff.manifest_hash != _workspace_diff_hash(diff.entries, schema_version=2):
        raise WorkspaceBoundaryError("candidate manifest hash or byte count differs")
    composed = dict(baseline)
    for path, entry in declared.items():
        if entry.operation == "delete":
            composed.pop(path)
        else:
            composed[path] = payload[path]
    _validate_symlink_chains(composed, max_depth)
    if _root_identities(lower, candidate) != roots_before:
        raise WorkspaceBoundaryError("workspace roots changed during validation")


def check_workspace_view_conflicts(lower_root, workspace_root, diff, *, max_bytes):
    workspace = Path(workspace_root)
    _nonoverlap(Path(lower_root), workspace, Path(diff.candidate_root))
    live = _inventory(workspace, max_entries=100_000, max_bytes=max_bytes, max_depth=256, live=True)
    preimages = _preimages(live)
    conflicts = []
    for entry in diff.entries:
        item = live.get(entry.path)
        if entry.operation == "add":
            if item is not None: conflicts.append(WorkspaceConflict(entry.path, "added_path_exists"))
        elif item is None or item.kind != entry.kind:
            conflicts.append(WorkspaceConflict(entry.path, "entry_type_changed"))
        elif preimages[entry.path] != entry.baseline_digest:
            conflicts.append(WorkspaceConflict(entry.path, "baseline_changed"))
    return WorkspaceConflictReport(len(diff.entries), tuple(conflicts))
