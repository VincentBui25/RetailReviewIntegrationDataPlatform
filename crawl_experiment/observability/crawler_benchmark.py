"""Append-only benchmark telemetry and resource sampling; never review payloads."""
from __future__ import annotations

import csv
import json
import math
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

SUCCESS = {"COMPLETE", "NO_REVIEWS", "PARTIAL_LIMIT", "PARTIAL_TIMEOUT"}
RETRIES = {"retry_navigation", "retry_fresh_session"}
PLACE_FIELDS = (
    "run_id", "store_id", "place_name", "google_place_id", "status", "started_at", "finished_at",
    "elapsed_seconds", "reviews_seen", "reviews_written", "reviews_per_second", "scroll_count",
    "new_reviews_per_scroll_avg", "retry_count", "limited_count", "challenge_count",
    "browser_restart_count", "browser_crash_count", "resolution_status", "error_type", "error_message",
)
SCROLL_FIELDS = (
    "run_id", "store_id", "place_name", "attempt", "scroll_index", "timestamp", "cards_found",
    "new_review_ids", "unique_review_ids", "scroll_top_before", "scroll_top_after", "scroll_height",
    "client_height", "scroll_action_elapsed_ms", "wait_for_growth_ms", "locate_cards_ms",
    "idle_count", "at_end", "reviews_per_scroll", "ms_per_new_review",
)
RESOURCE_FIELDS = (
    "timestamp", "run_id", "system_cpu_percent", "process_cpu_percent", "system_ram_used_mb",
    "system_ram_percent", "crawler_process_rss_mb", "crawler_process_vms_mb", "browser_process_count",
    "browser_rss_mb_total", "active_workers", "desired_concurrency", "crawler_tree_cpu_percent",
    "sample_error_count", "active_worker_count", "configured_concurrency", "pending_place_count",
)


def now():
    return datetime.now(timezone.utc).isoformat()


def divide(numerator, denominator):
    return numerator / denominator if denominator else 0.0


def percentile(values, fraction=0.95):
    values = sorted(values)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lo, hi = math.floor(position), math.ceil(position)
    return values[lo] + (values[hi] - values[lo]) * (position - lo)


def csv_write(path, rows, fields):
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


class BenchmarkMetrics:
    def __init__(self, directory, run_id, places, config, *, clock=time.monotonic):
        self.directory, self.run_id = Path(directory), run_id
        self.directory.mkdir(parents=True, exist_ok=True)
        self.config, self.clock = config, clock
        self.started_at, self.started_clock = now(), clock()
        self.lock = threading.RLock()
        self.places = {store_id: dict(run_id=run_id, store_id=store_id, place_name=name,
                                     status="QUEUED", reviews_seen=0, reviews_written=0, scroll_count=0,
                                     retry_count=0, limited_count=0, challenge_count=0,
                                     browser_restart_count=0, browser_crash_count=0)
                       for store_id, name in places}
        self.attempts, self.limited, self.challenges, self.retry_attempts = set(), set(), set(), set()
        self.resolved_places = set()
        self.seen, self.scroll_keys = defaultdict(set), set()
        self.active, self.context, self.starts = {}, {}, {}
        self.active_since, self.active_intervals = {}, []
        self.pending_places = set(self.places)
        self.scrolls, self.resources = [], []
        self.active_success_seconds = 0.0
        self.worker_errors = []
        self.browser_instances, self.crashed_instances, self.restarted_instances = set(), set(), set()
        self.sampler_errors = []
        self.persistence = {}
        self.events_stream = (self.directory / "events.jsonl").open("w", encoding="utf-8")
        self.scroll_stream = (self.directory / "scroll_metrics.csv").open("w", encoding="utf-8", newline="")
        self.scroll_writer = csv.DictWriter(self.scroll_stream, SCROLL_FIELDS)
        self.scroll_writer.writeheader()
        self.resource_stream = (self.directory / "resource_metrics.csv").open("w", encoding="utf-8", newline="")
        self.resource_writer = csv.DictWriter(self.resource_stream, RESOURCE_FIELDS)
        self.resource_writer.writeheader()

    def _event(self, event):
        self.events_stream.write(json.dumps(event, default=str, ensure_ascii=False) + "\n")
        self.events_stream.flush()

    def event(self, payload):
        with self.lock:
            event = dict(payload)
            worker = event.get("worker_id", "worker-1")
            context = self.context.get(worker, {})
            for key in ("store_id", "attempt", "browser_instance_id"):
                if event.get("event") == "worker_idle" and key in {"store_id", "attempt"}:
                    continue
                if event.get(key) is None:
                    event[key] = context.get(key)
            event.setdefault("timestamp", now())
            event["run_id"] = self.run_id
            if event.get("browser_instance_id") and not event["browser_instance_id"].startswith(worker + "/"):
                event["browser_instance_id"] = worker + "/" + event["browser_instance_id"]
            self._event(event)
            store_id, kind = event.get("store_id"), event["event"]
            place = self.places.get(store_id)
            event_clock = event.get("monotonic_seconds", self.clock())
            if kind == "worker_error":
                self.worker_errors.append(event)
            if kind == "resource_sampler_error":
                self.sampler_errors.append(event)
            if kind == "persistence_finalized":
                self.persistence[worker] = {key: event.get(key) for key in (
                    "state_db_path", "parquet_output_path", "parquet_files_written", "parquet_rows_written",
                    "new_reviews_written", "changed_reviews_written", "unchanged_reviews_seen")}
            if kind == "worker_finished":
                self._set_active(worker, False, event_clock)
            if kind == "place_claimed":
                self.pending_places.discard(store_id)
                self._set_active(worker, True, event_clock)
                self.context[worker] = dict(store_id=store_id, attempt=0,
                                            browser_instance_id=context.get("browser_instance_id"))
            if kind == "place_released":
                self._set_active(worker, False, event_clock)
                self.context[worker] = dict(browser_instance_id=context.get("browser_instance_id"))
            if place is None:
                return
            if kind == "worker_stalled":
                place.update(status="ERROR", error_type=event["error_type"], error_message=event["error_message"],
                             finished_at=event["timestamp"])
            attempt = event.get("attempt")
            key = (store_id, attempt)
            if kind == "attempt_started":
                self.attempts.add(key)
                self.context[worker] = dict(store_id=store_id, attempt=attempt,
                                            browser_instance_id=context.get("browser_instance_id"))
                self._set_active(worker, True, event_clock)
                self.starts.setdefault(store_id, self.clock())
                place.setdefault("started_at", event["timestamp"])
                place["status"] = "RUNNING"
            if kind in {"browser_created", "browser_reused"}:
                self.context.setdefault(worker, {}).update(browser_instance_id=event["browser_instance_id"])
                if kind == "browser_created":
                    self.browser_instances.add(event["browser_instance_id"])
            if kind in {"browser_crash", "browser_restarted"}:
                field = "browser_crash_count" if kind == "browser_crash" else "browser_restart_count"
                instances = self.crashed_instances if kind == "browser_crash" else self.restarted_instances
                identity = event.get("browser_instance_id") or (worker, store_id, attempt, kind)
                if identity not in instances:
                    instances.add(identity)
                    place[field] += 1
            if kind == "resolution":
                place["resolution_status"] = event["status"]
                if "google_place_id" in event:
                    place["google_place_id"] = event["google_place_id"]
                if event["status"] == "RESOLVED":
                    self.resolved_places.add(store_id)
            limited = kind in {"review_surface_limited", "navigation_limited"} or (
                kind == "retry_decision" and event.get("status") == "LIMITED")
            challenge = kind == "google_challenge" or (kind == "retry_decision" and event.get("status") == "CHALLENGE")
            if limited and key not in self.limited:
                self.limited.add(key)
                place["limited_count"] += 1
                if kind == "retry_decision":
                    self._event({**event, "event": "review_surface_limited" if event.get("error_type") == "LimitedReviewViewError" else "navigation_limited",
                                 "evidence": {"message": event.get("error_message")}})
            if challenge and key not in self.challenges:
                self.challenges.add(key)
                place["challenge_count"] += 1
            if kind == "retry_decision":
                if self.config.get("work_queue_mode") != "shared_dynamic":
                    self._set_active(worker, False, event_clock)
                place.update(error_type=event.get("error_type"), error_message=event.get("error_message"))
                if event.get("retry_action") in RETRIES and key not in self.retry_attempts:
                    self.retry_attempts.add(key)
                    place["retry_count"] += 1
            if kind == "review_observed":
                self.seen[store_id].add(event["source_review_id"])
                place["reviews_seen"] = len(self.seen[store_id])
                place["reviews_written"] += int(event["change_type"] in {"INSERT", "UPDATE"})
            if kind == "scroll_metric":
                self._scroll(event, place)
            if kind in {"place_finished", "place_failed"}:
                if self.config.get("work_queue_mode") != "shared_dynamic":
                    self._set_active(worker, False, event_clock)
                elapsed = max(0, self.clock() - self.starts.get(store_id, self.clock()))
                place.update(status=event["status"], finished_at=event["timestamp"], elapsed_seconds=elapsed)
                if kind == "place_failed":
                    place.update(error_type=event.get("error_type"), error_message=event.get("error_message"))
                if event["status"] in SUCCESS:
                    self.active_success_seconds += event.get("active_crawl_seconds", 0)
                place["reviews_per_second"] = divide(place["reviews_written"], elapsed)
            if kind != "review_observed":
                csv_write(self.directory / "place_metrics.csv", self.place_rows(), PLACE_FIELDS)

    def _scroll(self, event, place):
        key = (event["store_id"], event.get("attempt"), event.get("scroll_count"))
        if key in self.scroll_keys:
            return
        self.scroll_keys.add(key)
        new = event.get("new_review_ids", 0)
        row = {field: event.get(field) for field in SCROLL_FIELDS}
        row.update(place_name=place["place_name"], scroll_index=event.get("scroll_count"),
                   timestamp=event.get("scroll_started_at") or event["timestamp"],
                   unique_review_ids=event.get("reviews_after"), reviews_per_scroll=new,
                   ms_per_new_review=divide(event.get("iteration_elapsed_ms", 0), new) if new else None)
        self.scrolls.append(row)
        place["scroll_count"] += 1
        self.scroll_writer.writerow(row)
        self.scroll_stream.flush()

    def activity(self, worker_id, name, details):
        if name == "review_surface_classified" and details.get("access_state") == "LIMITED":
            self.event(dict(event="review_surface_limited", worker_id=worker_id,
                            store_id=details.get("store_id"), evidence=details.get("evidence")))
        elif name == "review_surface_classified":
            self.event(dict(event=name, worker_id=worker_id, **details))

    def resource(self, row):
        with self.lock:
            self.resources.append(row)
            self.resource_writer.writerow(row)
            self.resource_stream.flush()

    def active_workers(self):
        with self.lock:
            return sum(self.active.values())

    def _set_active(self, worker, active, timestamp):
        if bool(self.active.get(worker)) == active:
            return
        self.active[worker] = active
        if active:
            self.active_since[worker] = max(timestamp, self.started_clock)
        else:
            start = self.active_since.pop(worker)
            self.active_intervals.append((start, max(start, timestamp)))

    def pending_count(self):
        with self.lock:
            return len(self.pending_places)

    def place_rows(self):
        rows = []
        for place in self.places.values():
            row = dict(place)
            scrolls = [r for r in self.scrolls if r["store_id"] == row["store_id"]]
            row["new_reviews_per_scroll_avg"] = divide(sum(r["reviews_per_scroll"] for r in scrolls), len(scrolls))
            rows.append(row)
        return rows

    def finish(self):
        with self.lock:
            elapsed = max(0, self.clock() - self.started_clock)
            intervals = self.active_intervals + [(start, max(start, self.clock())) for start in self.active_since.values()]
            active_seconds = sum(end - start for start, end in intervals)
            transitions = sorted((point, change) for start, end in intervals if end > start
                                 for point, change in ((start, 1), (end, -1)))
            current_active, peak_active = 0, 0
            for _, change in transitions:
                current_active += change
                peak_active = max(peak_active, current_active)
            rows = self.place_rows()
            for row in rows:
                if row["status"] == "RUNNING":
                    row.update(status="ERROR", error_type="WorkerInterrupted",
                               error_message="No terminal worker result", finished_at=now(),
                               elapsed_seconds=max(0, self.clock() - self.starts[row["store_id"]]))
                elif row["status"] == "QUEUED":
                    row["status"] = "NOT_ATTEMPTED"
            attempted = len({key[0] for key in self.attempts})
            successful = sum(r["status"] in SUCCESS for r in rows)
            limited_places = len({key[0] for key in self.limited})
            challenge_places = len({key[0] for key in self.challenges})
            written = sum(r["reviews_written"] for r in rows)
            metrics = dict(run_id=self.run_id, started_at=self.started_at, finished_at=now(), elapsed_seconds=elapsed,
                           **self.config, places_attempted=attempted, places_resolved=len(self.resolved_places),
                           places_crawled_successfully=successful, places_failed=attempted - successful,
                           reviews_seen=sum(r["reviews_seen"] for r in rows), reviews_written=written,
                           reviews_per_second=divide(written, elapsed), places_per_hour=divide(successful * 3600, elapsed),
                           attempted_places_per_hour=divide(attempted * 3600, elapsed),
                           crawler_active_reviews_per_second=divide(written, self.active_success_seconds),
                           active_successful_crawl_seconds=self.active_success_seconds,
                           retry_count=len(self.retry_attempts), total_retry_count=len(self.retry_attempts),
                           retry_rate=divide(len(self.retry_attempts), len(self.attempts)),
                           attempt_retry_rate=divide(len(self.retry_attempts), len(self.attempts)),
                           total_place_attempts=len(self.attempts), places_retried=sum(r["retry_count"] > 0 for r in rows),
                           avg_retries_per_retried_place=divide(len(self.retry_attempts), sum(r["retry_count"] > 0 for r in rows)),
                           max_retries_for_place=max((r["retry_count"] for r in rows), default=0),
                           limited_count=limited_places, limited_place_count=limited_places, limited_attempt_count=len(self.limited),
                           limited_rate=divide(limited_places, attempted), challenge_count=challenge_places,
                           challenge_place_count=challenge_places, challenge_attempt_count=len(self.challenges),
                           challenge_rate=divide(challenge_places, attempted),
                           browser_crash_count=sum(r["browser_crash_count"] for r in rows),
                           browser_restart_count=sum(r["browser_restart_count"] for r in rows),
                           resource_sample_count=len(self.resources), worker_errors=self.worker_errors,
                           active_worker_seconds=active_seconds, avg_active_workers=divide(active_seconds, elapsed),
                           peak_active_workers=peak_active,
                           worker_utilization=divide(active_seconds, self.config["configured_concurrency"] * elapsed),
                           sampled_avg_active_workers=divide(sum(r.get("active_worker_count", 0) for r in self.resources), len(self.resources)),
                           unclaimed_place_count=len(self.pending_places),
                           resource_sampler_errors=self.sampler_errors, persistence=self.persistence,
                           parquet_rows_written=sum(p.get("parquet_rows_written") or 0 for p in self.persistence.values()),
                           browser_instances_created=len(self.browser_instances),
                           resource_scope="crawler descendant tree; CPU normalized to host logical CPU count",
                           rate_denominators={"retry_rate": "total retries / total place attempts (including retries)",
                                              "limited_rate": "distinct LIMITED places / distinct attempted places",
                                              "challenge_rate": "distinct CHALLENGE places / distinct attempted places"})
            for metric, field in (("cpu_percent", "crawler_tree_cpu_percent"), ("browser_process_count", "browser_process_count")):
                values = [r[field] for r in self.resources if r.get(field) is not None]
                metrics["avg_" + metric] = divide(sum(values), len(values)) if values else None
                metrics["p95_" + metric] = percentile(values)
                metrics["peak_" + metric] = max(values) if values else None
            ram = [r["crawler_process_rss_mb"] + r["browser_rss_mb_total"] for r in self.resources]
            metrics.update(avg_ram_mb=divide(sum(ram), len(ram)) if ram else None,
                           p95_ram_mb=percentile(ram), peak_ram_mb=max(ram) if ram else None)
            metrics["resource_measurement_warnings"] = []
            if self.browser_instances and not metrics["peak_browser_process_count"]:
                metrics["resource_measurement_warnings"].append(
                    "Browser sessions created but no browser descendants sampled: resource totals are incomplete")
            if self.sampler_errors or any(r.get("sample_error_count") for r in self.resources):
                metrics["resource_measurement_warnings"].append("Some resource reads failed; consult resource sampler events")
            csv_write(self.directory / "place_metrics.csv", rows, PLACE_FIELDS)
            path = self.directory / "run_metrics.json"
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(metrics, indent=2, default=str) + "\n", encoding="utf-8")
            tmp.replace(path)
            self._summary(metrics)
            return metrics

    def _summary(self, metrics):
        m = metrics
        ram_gb = divide(m["peak_ram_mb"] or 0, 1024)
        text = (
            f"# Crawler benchmark {self.run_id}\n\n"
            f"Configuration: concurrency={m['configured_concurrency']}, sample={len(self.places)}, "
            f"max_scrolls={m['configured_max_scrolls']}, timeout={m['configured_timeout_seconds']}s.\n\n"
            f"Concurrency: configured={m['configured_concurrency']}; time-weighted average active workers={m['avg_active_workers']:.3f}; "
            f"peak active workers={m['peak_active_workers']}; worker utilization={m['worker_utilization']:.2%}.\n\n"
            f"Throughput: {m['reviews_per_second']:.3f} written reviews/sec; {m['places_per_hour']:.2f} successful places/hour.\n\n"
            f"Resources (process tree): average/p95/peak CPU = {m['avg_cpu_percent']}/{m['p95_cpu_percent']}/{m['peak_cpu_percent']}; "
            f"average/p95/peak RAM MB = {m['avg_ram_mb']}/{m['p95_ram_mb']}/{m['peak_ram_mb']}.\n\n"
            f"Reliability: retry={m['retry_rate']:.2%}; LIMITED={m['limited_rate']:.2%}; "
            f"challenge={m['challenge_rate']:.2%}; crashes={m['browser_crash_count']}; restarts={m['browser_restart_count']}.\n\n"
            f"Efficiency (peak RAM GB denominator): written reviews/GB={divide(m['reviews_written'], ram_gb):.2f}; "
            f"places/hour/GB={divide(m['places_per_hour'], ram_gb):.2f}; "
            f"reviews/sec/concurrency={divide(m['reviews_per_second'], m['configured_concurrency']):.3f}.\n\n"
            "Retry denominator: all place attempts, including retries. LIMITED/challenge denominators: distinct attempted places.\n\n"
            "CPU is descendant-tree CPU divided by host logical CPU count. RAM is RSS summed across crawler and browser descendants; "
            "shared memory may be double-counted. Detached/non-descendant browser processes are not measurable here.\n\n"
            "No VM recommendation: compare multiple concurrency runs with identical samples before sizing.\n"
        )
        if m["resource_measurement_warnings"]:
            text += "\nMeasurement warnings:\n\n" + "\n".join("- " + warning for warning in m["resource_measurement_warnings"]) + "\n"
        (self.directory / "benchmark_summary.md").write_text(text, encoding="utf-8")

    def close(self):
        for stream in (self.events_stream, self.scroll_stream, self.resource_stream):
            stream.close()


class ResourceSampler:
    """One interruptible in-process thread, stopped/joined before collector close."""
    def __init__(self, metrics, *, interval=2, pid=None):
        if interval not in (2, 5):
            raise ValueError("resource sampling interval must be 2 or 5 seconds")
        import psutil
        self.psutil, self.metrics, self.interval = psutil, metrics, interval
        self.metrics.config.update(host_logical_cpus=psutil.cpu_count(),
                                   host_total_ram_mb=psutil.virtual_memory().total / 1048576,
                                   psutil_version=psutil.__version__)
        self.root = psutil.Process(pid or os.getpid())
        self.processes = {}
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="crawler-resource-sampler", daemon=False)

    def sample(self):
        psutil = self.psutil
        ram = psutil.virtual_memory()
        processes = [self.root] + self.root.children(recursive=True)
        cpu_primed = bool(self.processes)
        browser_count, browser_rss, rss, vms, tree_cpu, root_cpu, errors = 0, 0, 0, 0, 0, None, 0
        live = set()
        for discovered in processes:
            try:
                key = (discovered.pid, discovered.create_time())
                process = self.processes.setdefault(key, discovered)
                live.add(key)
                cpu = process.cpu_percent(interval=None)
                memory = process.memory_info()
                browser = any(token in process.name().lower() for token in ("chrome", "chromium", "msedge"))
                tree_cpu += cpu
                if process.pid == self.root.pid:
                    root_cpu = cpu
                if browser:
                    browser_count += 1
                    browser_rss += memory.rss
                else:
                    rss += memory.rss
                    vms += memory.vms
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                errors += 1
        self.processes = {key: value for key, value in self.processes.items() if key in live}
        active_workers = self.metrics.active_workers()
        return dict(timestamp=now(), run_id=self.metrics.run_id, system_cpu_percent=psutil.cpu_percent(),
                    process_cpu_percent=root_cpu if cpu_primed else None, system_ram_used_mb=ram.used / 1048576, system_ram_percent=ram.percent,
                    crawler_process_rss_mb=rss / 1048576, crawler_process_vms_mb=vms / 1048576,
                    browser_process_count=browser_count, browser_rss_mb_total=browser_rss / 1048576,
                    active_workers=active_workers, active_worker_count=active_workers,
                    configured_concurrency=self.metrics.config["configured_concurrency"],
                    pending_place_count=self.metrics.pending_count(), desired_concurrency=self.metrics.config["configured_concurrency"],
                    crawler_tree_cpu_percent=tree_cpu / (psutil.cpu_count() or 1) if cpu_primed else None, sample_error_count=errors)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self.metrics.resource(self.sample())
            except Exception as exc:
                self.metrics.event(dict(event="resource_sampler_error", error_type=type(exc).__name__, error_message=str(exc)))
            self.stop_event.wait(self.interval)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread.ident is not None:
            self.thread.join()
