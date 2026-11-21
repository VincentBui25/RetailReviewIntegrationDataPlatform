"""Shared local publication primitives, independent of dataset/business policy.

Callers supply their existing release reader/verifier and canonical serializers.
In particular, schema validation remains owned by the source domain verifier.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import uuid

import pyarrow.parquet as pq


def require(condition, message):
    if not condition:
        raise ValueError(message)


def _sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def check_hashes(hashes):
    for path, expected in hashes.items():
        require(_sha(path) == expected, "Pinned input/code changed: " + path)


def validate_publication_paths(output, inputs):
    """Reject overlapping resolved paths, not independent siblings of an input file.

    Fingerprints describe files (or explicitly supplied directories). A file's
    parent is not itself an input directory: campaign metadata can safely sit
    beside a separate publication subtree.
    """
    output = Path(output).resolve()
    for value in inputs:
        source = Path(value).resolve()
        require(not source.is_relative_to(output) and not output.is_relative_to(source),
                f"Output overlaps an input directory: output={output}; input={source}")


@contextmanager
def publication_lock(output):
    """OS lock released on process death; preserve the existing Human lock API."""
    path = output / ".publish.lock"
    with path.open("a+b") as stream:
        if path.stat().st_size == 0:
            stream.write(b"0"); stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise ValueError("Another human-decision publication is running") from error
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def publish_release(output, signature, writer, *, read_active, verify_release,
                    digest, dump, guard=lambda: None, fault=lambda stage: None,
                    lock=None):
    """Publish/reuse a complete immutable release with caller-owned validation.

The existing manifest layout (including production_gold=False), fault stages,
retained failed stages and final guards are unchanged. No source schema or
acceptance policy is inferred here. Serializers are passed through unchanged.
"""
    output = Path(output).resolve()
    fingerprints = {**signature["inputs_sha256"], **signature["code_sha256"]}
    validate_publication_paths(output, fingerprints)
    rid = digest(signature)[:24]
    output.mkdir(parents=True, exist_ok=True)
    with (lock or publication_lock)(output):
        current = read_active(output)
        if current:
            require(verify_release(current)["signature"]["rule_version"] == signature["rule_version"], "Different dataset at publication root")
        dest = output / "releases" / rid
        if not dest.exists():
            stage = output / (".stage-" + uuid.uuid4().hex)
            stage.mkdir()
            tables, summary, extra = writer(stage, rid)
            artifacts = {}
            for name, table in tables.items():
                pq.write_table(table, stage / name, compression="zstd")
                artifacts[name] = dict(rows=table.num_rows, sha256=_sha(stage / name), logical_sha256=digest(table.to_pylist()), schema=str(table.schema))
            (stage / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            for name in ["summary.json", *extra]:
                require((stage / name).is_file(), "Incomplete staging artifact: " + name)
                artifacts[name] = dict(sha256=_sha(stage / name))
                if name.endswith('.parquet'):
                    extra_table = pq.read_table(stage / name)
                    artifacts[name].update(rows=extra_table.num_rows, logical_sha256=digest(extra_table.to_pylist()), schema=str(extra_table.schema))
            (stage / "manifest.json").write_text(dump(dict(release_id=rid, signature=signature, artifacts=artifacts,
                publication_kind=signature["publication_kind"], production_gold=False)), encoding="utf-8")
            verify_release(stage); fault("after_stage_validation")
            check_hashes(fingerprints); guard()
            dest.parent.mkdir(exist_ok=True); os.replace(stage, dest)
            fault("after_release_rename")
        else:
            require(verify_release(dest)["signature"] == signature, "Release signature mismatch")
        fault("before_pointer_replace"); check_hashes(fingerprints); guard()
        pointer = output / (".current-" + uuid.uuid4().hex)
        pointer.write_text(dump(dict(release_id=rid, manifest_sha256=_sha(dest / "manifest.json"))), encoding="utf-8")
        os.replace(pointer, output / "current.json"); fault("after_pointer_replace")
        return dest, json.loads((dest / "summary.json").read_text(encoding="utf-8"))
