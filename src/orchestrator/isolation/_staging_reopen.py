"""Owner-aware existing-root acquisition; returned roots delegate one close path."""
import os
from pathlib import Path

from ._staging_a2_contract import NativePhase, Role
from ._staging_arena import (
    ArenaUnavailable, FixtureRoots, _DIR_FLAGS, _ROOT_NAME, _check_owned,
    _directory_matches, _fd_directory, _identity,
)
from ._staging_contract import _require
from ._staging_ownership import AcquisitionLedger, FDKind


def _open_owned(path, *, owner, dir_fd=None):
    fd = os.open(path, _DIR_FLAGS, dir_fd=dir_fd)
    cap = owner.adopt_fd(fd, role=Role.HOST, identity=None, kind=FDKind.DIRECTORY)
    info = os.fstat(fd)
    owner.bind_identity(cap, _identity(info))
    return cap


def open_fixture_roots_existing(parent: Path, *, owner: AcquisitionLedger) -> FixtureRoots:
    _require(isinstance(parent, Path) and parent.is_absolute() and '..' not in parent.parts
             and type(owner) is AcquisitionLedger)
    try:
        current = _open_owned('/', owner=owner)
        for component in parent.parts[1:]:
            following = _open_owned(component, owner=owner, dir_fd=current.fd)
            owner.close_once(current)
            current = following
        parent_cap = current
        parent_fd = parent_cap.fd
        _fd_directory(parent_fd)
        allowed = {'staging-fixture', 'staging-a2-baseline-v2.bin', 'staging-a2-retention-v2.bin'}
        seen = set()
        with os.scandir(parent_fd) as entries:
            for _ in range(4):
                item = next(entries, None)
                if item is None:
                    break
                _require(item.name in allowed and item.name not in seen)
                seen.add(item.name)
            else:
                _require(False)
        _require(_ROOT_NAME in seen)
        for leaf, size in (('staging-a2-baseline-v2.bin', 1024), ('staging-a2-retention-v2.bin', 512)):
            if leaf in seen:
                info = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                _check_owned(info, directory=False)
                _require(info.st_size == size)
        root_cap = _open_owned(_ROOT_NAME, owner=owner, dir_fd=parent_fd)
        _fd_directory(root_cap.fd)
        _require(_directory_matches(root_cap.fd, {'source', 'arena'}))
        source_cap = _open_owned('source', owner=owner, dir_fd=root_cap.fd)
        _fd_directory(source_cap.fd)
        arena_cap = _open_owned('arena', owner=owner, dir_fd=root_cap.fd)
        _fd_directory(arena_cap.fd)
        caps = (parent_cap, root_cap, source_cap, arena_cap)
        identities = tuple(owner._record(cap).identity for cap in caps)
        roots = FixtureRoots(source_cap.fd, arena_cap.fd, root_cap.fd, parent_fd, identities)
        roots.validate()
        owner.bind_fixture_roots(roots, caps)
        return roots
    except BaseException:
        owner.poison_debt(NativePhase.REOPEN_ROOTS)
        raise
