//! Thin PyO3 translation layer over the thread-safe WP-05 engine facade.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use pyo3::create_exception;
use pyo3::exceptions::{PyIOError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList};
use salamander_db::{
    BranchDto, DiffRequestDto, DiffSideDto, DurabilityDto, Engine, EngineAppendBatch, EngineError,
    EngineOptions, ErrorCategory, EventData, ExpectedRevisionDto, FeedFilter, FeedRequest,
    QueryDefinition, QueryHandle, QueryOperation, RecordDto, ReplayRequest,
};
use serde_json::Value;

/// Metadata key carrying the user-facing stream (namespace) name on every
/// appended record — the same key the facade's paged replay filters on.
const STREAM_NAME_KEY: &str = "salamander.stream_name";

/// Upper bound on one blocking wait inside `Watch.__next__` before the GIL
/// is retaken to deliver pending signals — keeps Ctrl+C responsive while a
/// watch blocks indefinitely.
const WATCH_WAIT_CHUNK_MILLIS: u64 = 200;

create_exception!(salamander, SalamanderError, PyRuntimeError);
create_exception!(salamander, InvalidArgumentError, PyValueError);
create_exception!(salamander, ConflictError, PyValueError);
create_exception!(salamander, NotFoundError, PyKeyError);
create_exception!(salamander, LockedError, PyIOError);
create_exception!(salamander, IoError, PyIOError);
create_exception!(salamander, CorruptionError, PyRuntimeError);
create_exception!(salamander, UnsupportedFormatError, PyRuntimeError);
create_exception!(salamander, CodecError, PyValueError);
create_exception!(salamander, ResourceLimitError, PyValueError);
create_exception!(salamander, CancelledError, PyRuntimeError);

#[pyclass]
struct Salamander {
    engine: Engine,
}

#[pymethods]
impl Salamander {
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, commit_every_bytes=None, commit_every_count=None, commit_every_millis=None, snapshot_every_events=None, snapshot_every_bytes=None, snapshot_every_millis=None))]
    fn open(
        py: Python<'_>,
        path: &str,
        commit_every_bytes: Option<u64>,
        commit_every_count: Option<u64>,
        commit_every_millis: Option<u64>,
        snapshot_every_events: Option<u64>,
        snapshot_every_bytes: Option<u64>,
        snapshot_every_millis: Option<u64>,
    ) -> PyResult<Self> {
        let mut options = EngineOptions::new(path);
        options.commit_every_bytes = commit_every_bytes;
        options.commit_every_count = commit_every_count;
        options.commit_every_millis = commit_every_millis;
        options.snapshot_every_events = snapshot_every_events;
        options.snapshot_every_bytes = snapshot_every_bytes;
        options.snapshot_every_millis = snapshot_every_millis;
        let engine = py
            .allow_threads(|| Engine::open(options))
            .map_err(to_pyerr)?;
        Ok(Self { engine })
    }

    fn append(&self, py: Python<'_>, namespace: &str, event: &Bound<'_, PyAny>) -> PyResult<u64> {
        let payload = value_bytes(&py_to_value(event)?)?;
        py.allow_threads(|| self.engine.append(json_batch([0; 16], namespace, payload)))
            .map(|receipt| receipt.first_position)
            .map_err(to_pyerr)
    }

    /// Append one JSON event and return the complete stable engine receipt.
    fn append_receipt(
        &self,
        py: Python<'_>,
        namespace: &str,
        event: &Bound<'_, PyAny>,
    ) -> PyResult<PyObject> {
        let payload = value_bytes(&py_to_value(event)?)?;
        let receipt = py
            .allow_threads(|| self.engine.append(json_batch([0; 16], namespace, payload)))
            .map_err(to_pyerr)?;
        receipt_to_py(py, &receipt)
    }

    /// Atomically append a batch of fully described JSON events.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (namespace, events, *, branch=None, expected_revision=None, idempotency_key=None, durability="buffered"))]
    fn append_batch(
        &self,
        py: Python<'_>,
        namespace: &str,
        events: &Bound<'_, PyList>,
        branch: Option<&str>,
        expected_revision: Option<Bound<'_, PyAny>>,
        idempotency_key: Option<Bound<'_, PyAny>>,
        durability: &str,
    ) -> PyResult<PyObject> {
        let branch_id = match branch {
            Some(name) => {
                py.allow_threads(|| self.engine.branch_named(name.to_string()))
                    .map_err(to_pyerr)?
                    .id
            }
            None => [0; 16],
        };
        let request = EngineAppendBatch {
            branch_id,
            stream: namespace.to_string(),
            expected: parse_expected_revision(expected_revision.as_ref())?,
            idempotency_key: idempotency_key.as_ref().map(bytes_or_utf8).transpose()?,
            events: events
                .iter()
                .map(|event| event_data(&event))
                .collect::<PyResult<_>>()?,
            durability: parse_durability(durability)?,
        };
        let receipt = py
            .allow_threads(|| self.engine.append(request))
            .map_err(to_pyerr)?;
        receipt_to_py(py, &receipt)
    }

    fn append_branch(
        &self,
        py: Python<'_>,
        branch: &str,
        namespace: &str,
        event: &Bound<'_, PyAny>,
    ) -> PyResult<u64> {
        let payload = value_bytes(&py_to_value(event)?)?;
        let info = py
            .allow_threads(|| self.engine.branch_named(branch.to_string()))
            .map_err(to_pyerr)?;
        py.allow_threads(|| self.engine.append(json_batch(info.id, namespace, payload)))
            .map(|receipt| receipt.first_position)
            .map_err(to_pyerr)
    }

    fn commit(&self, py: Python<'_>) -> PyResult<u64> {
        py.allow_threads(|| self.engine.commit()).map_err(to_pyerr)
    }

    fn head(&self, py: Python<'_>) -> PyResult<u64> {
        py.allow_threads(|| self.engine.head()).map_err(to_pyerr)
    }

    fn durable_head(&self, py: Python<'_>) -> PyResult<u64> {
        py.allow_threads(|| self.engine.durable_head())
            .map_err(to_pyerr)
    }

    fn uncommitted_count(&self, py: Python<'_>) -> PyResult<u64> {
        py.allow_threads(|| self.engine.uncommitted_count())
            .map_err(to_pyerr)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.engine.close()).map_err(to_pyerr)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    #[pyo3(signature = (namespace, start=0, end=None))]
    fn replay(
        &self,
        py: Python<'_>,
        namespace: &str,
        start: u64,
        end: Option<u64>,
    ) -> PyResult<Py<PyList>> {
        let rows = py
            .allow_threads(|| collect(&self.engine, [0; 16], namespace, start, end))
            .map_err(to_pyerr)?;
        rows_to_pylist(py, &rows)
    }

    #[pyo3(signature = (namespace, start=0, end=None, page_events=256, page_bytes=1048576))]
    fn open_reader(
        &self,
        py: Python<'_>,
        namespace: &str,
        start: u64,
        end: Option<u64>,
        page_events: u32,
        page_bytes: usize,
    ) -> PyResult<Reader> {
        let engine = self.engine.clone();
        let handle = py
            .allow_threads(|| {
                engine.open_reader(ReplayRequest {
                    branch_id: [0; 16],
                    stream: Some(namespace.into()),
                    from: start,
                    until: end,
                    page_events,
                    page_bytes,
                })
            })
            .map_err(to_pyerr)?;
        Ok(Reader {
            engine: self.engine.clone(),
            handle: Some(handle),
        })
    }

    #[pyo3(signature = (name, key, indexes=None, where_field=None, where_value=None))]
    fn register_view(
        &self,
        py: Python<'_>,
        name: &str,
        key: &str,
        indexes: Option<BTreeMap<String, String>>,
        where_field: Option<String>,
        where_value: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let filter = match (where_field, where_value) {
            (Some(field), Some(value)) => Some((field, value_bytes(&py_to_value(&value)?)?)),
            _ => None,
        };
        py.allow_threads(|| {
            self.engine.register_query(
                name.to_string(),
                QueryDefinition {
                    key_field: key.to_string(),
                    indexes: indexes.unwrap_or_default(),
                    filter,
                },
            )
        })
        .map(|_| ())
        .map_err(to_pyerr)
    }

    fn deregister_view(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        py.allow_threads(|| self.engine.remove_query(name.to_string()))
            .map_err(to_pyerr)
    }

    fn view(slf: Bound<'_, Self>, py: Python<'_>, name: &str) -> PyResult<View> {
        let engine = slf.borrow().engine.clone();
        let handle = py
            .allow_threads(|| engine.query_named(name.to_string()))
            .map_err(to_pyerr)?;
        Ok(View {
            parent: slf.unbind(),
            handle,
            name: name.to_string(),
        })
    }

    #[pyo3(signature = (namespace, at, parent=None))]
    fn fork(
        &self,
        py: Python<'_>,
        namespace: &str,
        at: u64,
        parent: Option<&str>,
    ) -> PyResult<String> {
        py.allow_threads(|| -> Result<_, EngineError> {
            // The engine forks from any parent branch; default to the root
            // timeline when no parent is named, so fork-of-a-fork just works.
            let parent_id = match parent {
                None => [0; 16],
                Some(name) => self.engine.branch_named(name.to_string())?.id,
            };
            self.engine.fork(
                parent_id,
                at,
                format!("{namespace}-fork-{at}"),
                BTreeMap::new(),
            )
        })
        .map(|branch| branch.name)
        .map_err(to_pyerr)
    }

    /// A blocking iterator over events as they become durable — `tail -f`
    /// for the log, over the engine's committed-batch feed. `start=None`
    /// tails live from the durable head (or resumes a `consumer_id`'s
    /// acknowledged checkpoint); `start=0` replays all durable history
    /// first, then follows. `timeout` (seconds) ends the iteration after
    /// that long without a matching event; `None` blocks until closed.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (namespace=None, branch=None, start=None, consumer_id=None, timeout=None, page_batches=128, page_bytes=1048576))]
    fn watch(
        &self,
        py: Python<'_>,
        namespace: Option<String>,
        branch: Option<&str>,
        start: Option<u64>,
        consumer_id: Option<String>,
        timeout: Option<f64>,
        page_batches: u32,
        page_bytes: usize,
    ) -> PyResult<Watch> {
        let branches = match branch {
            None => Vec::new(),
            Some(name) => vec![
                py.allow_threads(|| self.engine.branch_named(name.to_string()))
                    .map_err(to_pyerr)?
                    .id,
            ],
        };
        let from = match (start, &consumer_id) {
            (Some(position), _) => Some(position),
            (None, Some(_)) => None, // resume from the acknowledged checkpoint
            (None, None) => Some(
                py.allow_threads(|| self.engine.durable_head())
                    .map_err(to_pyerr)?,
            ),
        };
        let handle = py
            .allow_threads(|| {
                self.engine.open_feed(FeedRequest {
                    from,
                    consumer_id,
                    filter: FeedFilter {
                        branches,
                        streams: Vec::new(),
                        event_types: Vec::new(),
                    },
                    page_batches,
                    page_bytes,
                })
            })
            .map_err(to_pyerr)?;
        Ok(Watch {
            engine: self.engine.clone(),
            handle: Some(handle),
            namespace,
            timeout_millis: timeout.map(|seconds| (seconds * 1000.0).max(0.0) as u64),
            buffer: VecDeque::new(),
        })
    }

    /// The divergence of two timelines (branch names; "main" is the
    /// default timeline): a summary dict plus three pre-scoped `Reader`s —
    /// nothing is materialized until a reader is drained. See
    /// docs/specs/first-class-diff.md.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (left, right, namespace=None, left_until=None, right_until=None, page_events=256, page_bytes=1048576))]
    fn diff(
        &self,
        py: Python<'_>,
        left: &str,
        right: &str,
        namespace: Option<&str>,
        left_until: Option<u64>,
        right_until: Option<u64>,
        page_events: u32,
        page_bytes: usize,
    ) -> PyResult<PyObject> {
        let diff = py
            .allow_threads(|| -> Result<_, EngineError> {
                let left = self.engine.branch_named(left.to_string())?.id;
                let right = self.engine.branch_named(right.to_string())?.id;
                self.engine.diff(DiffRequestDto {
                    left_branch_id: left,
                    right_branch_id: right,
                    left_until,
                    right_until,
                    stream: namespace.map(str::to_string),
                    page_events,
                    page_bytes,
                })
            })
            .map_err(to_pyerr)?;
        let reader = |request: ReplayRequest| -> PyResult<Py<Reader>> {
            let handle = py
                .allow_threads(|| self.engine.open_reader(request))
                .map_err(to_pyerr)?;
            Py::new(
                py,
                Reader {
                    engine: self.engine.clone(),
                    handle: Some(handle),
                },
            )
        };
        let side = |side: &DiffSideDto| -> PyResult<PyObject> {
            let out = PyDict::new(py);
            out.set_item("branch", &side.branch.name)?;
            out.set_item("until", side.until)?;
            out.set_item("suffix", reader(side.suffix.clone())?)?;
            Ok(out.into_any().unbind())
        };
        let out = PyDict::new(py);
        out.set_item("common_ancestor", &diff.common_ancestor.name)?;
        out.set_item("divergence_offset", diff.divergence_position)?;
        out.set_item("shared", reader(diff.shared.clone())?)?;
        out.set_item("left", side(&diff.left)?)?;
        out.set_item("right", side(&diff.right)?)?;
        Ok(out.into_any().unbind())
    }

    fn history(&self, py: Python<'_>, namespace: &str) -> PyResult<Py<PyList>> {
        let rows = py
            .allow_threads(|| collect(&self.engine, [0; 16], namespace, 0, None))
            .map_err(to_pyerr)?;
        rows_to_pylist(py, &rows)
    }

    fn branch_history(
        &self,
        py: Python<'_>,
        branch: &str,
        namespace: &str,
    ) -> PyResult<Py<PyList>> {
        let branch = py
            .allow_threads(|| self.engine.branch_named(branch.to_string()))
            .map_err(to_pyerr)?;
        let rows = py
            .allow_threads(|| collect(&self.engine, branch.id, namespace, 0, None))
            .map_err(to_pyerr)?;
        rows_to_pylist(py, &rows)
    }

    fn branch_ancestry(&self, py: Python<'_>, branch: &str) -> PyResult<Py<PyList>> {
        let branch = py
            .allow_threads(|| self.engine.branch_named(branch.to_string()))
            .map_err(to_pyerr)?;
        let ancestry = py
            .allow_threads(|| self.engine.ancestry(branch.id))
            .map_err(to_pyerr)?;
        let out = PyList::empty(py);
        for info in ancestry {
            out.append(branch_info_to_py(py, &info)?)?;
        }
        Ok(out.unbind())
    }

    fn archive_branch(&self, py: Python<'_>, branch: &str) -> PyResult<PyObject> {
        let branch = py
            .allow_threads(|| self.engine.branch_named(branch.to_string()))
            .map_err(to_pyerr)?;
        let info = py
            .allow_threads(|| self.engine.archive(branch.id))
            .map_err(to_pyerr)?;
        branch_info_to_py(py, &info)
    }

    fn create_snapshot(&self, py: Python<'_>, projection: &str) -> PyResult<PyObject> {
        let handle = py
            .allow_threads(|| self.engine.query_named(projection.to_string()))
            .map_err(to_pyerr)?;
        let info = py
            .allow_threads(|| self.engine.create_snapshot(handle))
            .map_err(to_pyerr)?;
        snapshot_info_to_py(py, &info)
    }

    fn list_snapshots(&self, py: Python<'_>, projection: &str) -> PyResult<Py<PyList>> {
        let handle = py
            .allow_threads(|| self.engine.query_named(projection.to_string()))
            .map_err(to_pyerr)?;
        let snapshots = py
            .allow_threads(|| self.engine.list_snapshots(handle))
            .map_err(to_pyerr)?;
        let out = PyList::empty(py);
        for info in snapshots {
            out.append(snapshot_info_to_py(py, &info)?)?;
        }
        Ok(out.unbind())
    }

    fn verify_snapshot(&self, py: Python<'_>, snapshot_id: &str) -> PyResult<PyObject> {
        let info = py
            .allow_threads(|| self.engine.verify_snapshot(snapshot_id.to_string()))
            .map_err(to_pyerr)?;
        snapshot_info_to_py(py, &info)
    }

    fn delete_snapshot(&self, py: Python<'_>, snapshot_id: &str) -> PyResult<bool> {
        py.allow_threads(|| self.engine.delete_snapshot(snapshot_id.to_string()))
            .map_err(to_pyerr)
    }

    fn delete_all_derived_state(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.engine.delete_all_derived_state())
            .map_err(to_pyerr)
    }

    fn rebuild_projection(&self, py: Python<'_>, projection: &str) -> PyResult<()> {
        let handle = py
            .allow_threads(|| self.engine.query_named(projection.to_string()))
            .map_err(to_pyerr)?;
        py.allow_threads(|| self.engine.rebuild_projection(handle))
            .map_err(to_pyerr)
    }

    fn __repr__(&self) -> String {
        match self.engine.head() {
            Ok(head) => format!("<salamander.Salamander head={head}>"),
            Err(_) => "<salamander.Salamander closed>".into(),
        }
    }
}

#[pyclass]
struct View {
    parent: Py<Salamander>,
    handle: QueryHandle,
    name: String,
}

#[pyclass]
struct Reader {
    engine: Engine,
    handle: Option<salamander_db::ReaderHandle>,
}

/// A blocking iterator over events as they become durable — the
/// committed-batch feed worn as `tail -f`. Yields the same row dicts as
/// `replay`, releases the GIL while waiting, and stays responsive to
/// Ctrl+C by waking every `WATCH_WAIT_CHUNK_MILLIS` to deliver signals.
#[pyclass]
struct Watch {
    engine: Engine,
    handle: Option<salamander_db::FeedHandle>,
    /// Row-level stream-name filter (batch feeds keep original batch
    /// boundaries, so namespace selection happens per event, exactly like
    /// the facade's paged replay).
    namespace: Option<String>,
    /// `None` blocks forever; otherwise `__next__` ends the iteration
    /// (StopIteration) when no matching event arrives within the window.
    timeout_millis: Option<u64>,
    buffer: VecDeque<RecordDto>,
}

#[pymethods]
impl Watch {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let deadline = self
            .timeout_millis
            .map(|millis| Instant::now() + Duration::from_millis(millis));
        loop {
            if let Some(row) = self.buffer.pop_front() {
                return Ok(Some(row_to_py(py, &row)?));
            }
            let handle = self
                .handle
                .ok_or_else(|| PyRuntimeError::new_err("watch is closed"))?;
            let wait = match deadline {
                None => WATCH_WAIT_CHUNK_MILLIS,
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Ok(None); // idle past the timeout — StopIteration
                    };
                    (remaining.as_millis() as u64).min(WATCH_WAIT_CHUNK_MILLIS)
                }
            };
            let engine = self.engine.clone();
            let page = py
                .allow_threads(|| engine.next_feed_page(handle, Some(wait)))
                .map_err(to_pyerr)?;
            for batch in page.batches {
                for event in batch.events {
                    let matches = match &self.namespace {
                        None => true,
                        Some(wanted) => record_namespace(&event) == Some(wanted.as_str()),
                    };
                    if matches {
                        self.buffer.push_back(event);
                    }
                }
            }
            py.check_signals()?;
            if self.buffer.is_empty() {
                if let Some(deadline) = deadline {
                    if Instant::now() >= deadline {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Persists the consumer checkpoint at the current feed position, so a
    /// later `db.watch(consumer_id=...)` resumes exactly here. Meaningful
    /// only when the watch was opened with a `consumer_id`.
    fn ack(&self, py: Python<'_>) -> PyResult<u64> {
        let handle = self
            .handle
            .ok_or_else(|| PyRuntimeError::new_err("watch is closed"))?;
        py.allow_threads(|| self.engine.acknowledge_feed(handle))
            .map_err(to_pyerr)
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            py.allow_threads(|| self.engine.close_feed(handle))
                .map_err(to_pyerr)?;
        }
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.engine.close_feed(handle);
        }
    }
}

#[pymethods]
impl Reader {
    fn next_page(&self, py: Python<'_>) -> PyResult<PyObject> {
        let handle = self
            .handle
            .ok_or_else(|| PyRuntimeError::new_err("reader is closed"))?;
        let page = py
            .allow_threads(|| self.engine.next_page(handle))
            .map_err(to_pyerr)?;
        let out = PyDict::new(py);
        out.set_item("records", rows_to_pylist(py, &page.records)?)?;
        out.set_item("continuation", page.continuation)?;
        out.set_item("done", page.done)?;
        Ok(out.into_any().unbind())
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let handle = self
            .handle
            .ok_or_else(|| PyRuntimeError::new_err("reader is closed"))?;
        py.allow_threads(|| self.engine.cancel_reader(handle))
            .map_err(to_pyerr)
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            py.allow_threads(|| self.engine.close_reader(handle))
                .map_err(to_pyerr)?;
        }
        Ok(())
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.engine.close_reader(handle);
        }
    }
}

#[pymethods]
impl View {
    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Option<PyObject>> {
        let engine = self.parent.bind(py).borrow().engine.clone();
        let result = py
            .allow_threads(|| engine.query(self.handle, QueryOperation::Get(key.into())))
            .map_err(to_pyerr)?;
        result
            .rows
            .first()
            .map(|row| bytes_to_py(py, row))
            .transpose()
    }

    fn by(&self, py: Python<'_>, index: &str, key: &Bound<'_, PyAny>) -> PyResult<Py<PyList>> {
        let key = index_key_bytes(&py_to_value(key)?);
        let engine = self.parent.bind(py).borrow().engine.clone();
        let result = py
            .allow_threads(|| {
                engine.query(
                    self.handle,
                    QueryOperation::By {
                        index: index.into(),
                        key,
                    },
                )
            })
            .map_err(to_pyerr)?;
        bytes_rows_to_pylist(py, &result.rows)
    }

    fn range(&self, py: Python<'_>, lo: String, hi: String) -> PyResult<Py<PyList>> {
        let engine = self.parent.bind(py).borrow().engine.clone();
        let result = py
            .allow_threads(|| {
                engine.query(self.handle, QueryOperation::Range { start: lo, end: hi })
            })
            .map_err(to_pyerr)?;
        bytes_rows_to_pylist(py, &result.rows)
    }

    fn prefix(&self, py: Python<'_>, prefix: &str) -> PyResult<Py<PyList>> {
        let engine = self.parent.bind(py).borrow().engine.clone();
        let result = py
            .allow_threads(|| engine.query(self.handle, QueryOperation::Prefix(prefix.into())))
            .map_err(to_pyerr)?;
        bytes_rows_to_pylist(py, &result.rows)
    }

    fn len(&self, py: Python<'_>) -> PyResult<u64> {
        let engine = self.parent.bind(py).borrow().engine.clone();
        py.allow_threads(|| engine.query(self.handle, QueryOperation::Len))
            .map(|result| result.len)
            .map_err(to_pyerr)
    }

    fn __repr__(&self) -> String {
        format!("<salamander.View {:?}>", self.name)
    }
}

fn json_batch(branch_id: [u8; 16], stream: &str, payload: Vec<u8>) -> EngineAppendBatch {
    EngineAppendBatch {
        branch_id,
        stream: stream.to_string(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        events: vec![EventData::json(payload)],
        durability: DurabilityDto::Buffered,
    }
}

fn event_data(event: &Bound<'_, PyAny>) -> PyResult<EventData> {
    let descriptor = event.downcast::<PyDict>().map_err(|_| {
        InvalidArgumentError::new_err("each batch event must be a dict with a 'body' field")
    })?;
    let body = descriptor
        .get_item("body")?
        .ok_or_else(|| InvalidArgumentError::new_err("batch event is missing required 'body'"))?;
    let event_type = descriptor
        .get_item("event_type")?
        .map(|value| value.extract::<String>())
        .transpose()?
        .unwrap_or_else(|| "application.json".into());
    let schema_version = descriptor
        .get_item("schema_version")?
        .map(|value| value.extract::<u32>())
        .transpose()?
        .unwrap_or(1);
    let event_id = descriptor
        .get_item("event_id")?
        .map(|value| {
            value
                .extract::<String>()
                .and_then(|value| parse_hex_id(&value))
        })
        .transpose()?;
    let metadata = descriptor
        .get_item("metadata")?
        .map(|value| parse_metadata(&value))
        .transpose()?
        .unwrap_or_default();
    Ok(EventData {
        event_id,
        event_type,
        schema_version,
        metadata,
        codec: salamander_db::PayloadCodec::Json,
        payload: value_bytes(&py_to_value(&body)?)?,
    })
}

fn parse_expected_revision(value: Option<&Bound<'_, PyAny>>) -> PyResult<ExpectedRevisionDto> {
    let Some(value) = value else {
        return Ok(ExpectedRevisionDto::Any);
    };
    if value.is_none() {
        return Ok(ExpectedRevisionDto::Any);
    }
    if value.downcast::<PyBool>().is_ok() {
        return Err(InvalidArgumentError::new_err(
            "expected_revision must be None, 'any', 'no_stream', or a non-negative integer",
        ));
    }
    if let Ok(revision) = value.extract::<u64>() {
        return Ok(ExpectedRevisionDto::Exact(revision));
    }
    if let Ok(value) = value.extract::<String>() {
        match value.as_str() {
            "any" => return Ok(ExpectedRevisionDto::Any),
            "no_stream" => return Ok(ExpectedRevisionDto::NoStream),
            _ => {}
        }
    }
    Err(InvalidArgumentError::new_err(
        "expected_revision must be None, 'any', 'no_stream', or a non-negative integer",
    ))
}

fn parse_durability(value: &str) -> PyResult<DurabilityDto> {
    match value {
        "buffered" => Ok(DurabilityDto::Buffered),
        "flush" => Ok(DurabilityDto::Flush),
        "sync" => Ok(DurabilityDto::Sync),
        _ => Err(InvalidArgumentError::new_err(
            "durability must be 'buffered', 'flush', or 'sync'",
        )),
    }
}

fn parse_metadata(value: &Bound<'_, PyAny>) -> PyResult<BTreeMap<String, Vec<u8>>> {
    let metadata = value
        .downcast::<PyDict>()
        .map_err(|_| InvalidArgumentError::new_err("event metadata must be a dict"))?;
    metadata
        .iter()
        .map(|(key, value)| Ok((key.extract::<String>()?, bytes_or_utf8(&value)?)))
        .collect()
}

fn bytes_or_utf8(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(value) = value.downcast::<PyBytes>() {
        return Ok(value.as_bytes().to_vec());
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(value.into_bytes());
    }
    Err(InvalidArgumentError::new_err(
        "value must be bytes or a UTF-8 string",
    ))
}

fn parse_hex_id(value: &str) -> PyResult<[u8; 16]> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(InvalidArgumentError::new_err(
            "event_id must contain exactly 32 hexadecimal characters",
        ));
    }
    let mut id = [0; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| InvalidArgumentError::new_err("event_id is not valid hexadecimal"))?;
    }
    Ok(id)
}

fn collect(
    engine: &Engine,
    branch_id: [u8; 16],
    stream: &str,
    from: u64,
    until: Option<u64>,
) -> Result<Vec<RecordDto>, EngineError> {
    let handle = engine.open_reader(ReplayRequest {
        branch_id,
        stream: Some(stream.into()),
        from,
        until,
        page_events: 1024,
        page_bytes: 8 * 1024 * 1024,
    })?;
    let mut records = Vec::new();
    loop {
        let page = engine.next_page(handle)?;
        records.extend(page.records);
        if page.done {
            break;
        }
    }
    engine.close_reader(handle)?;
    Ok(records)
}

fn row_to_py(py: Python<'_>, row: &RecordDto) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("offset", row.position)?;
    dict.set_item("timestamp_ms", row.timestamp_unix_nanos / 1_000_000)?;
    dict.set_item("event_id", hex_id(row.event_id))?;
    dict.set_item("batch_id", hex_id(row.batch_id))?;
    dict.set_item("batch_index", row.batch_index)?;
    dict.set_item("branch_id", hex_id(row.branch_id))?;
    dict.set_item("namespace", record_namespace(row))?;
    dict.set_item("stream_revision", row.stream_revision)?;
    dict.set_item("event_type", &row.event_type)?;
    dict.set_item("schema_version", row.schema_version)?;
    dict.set_item(
        "codec",
        match row.codec {
            salamander_db::PayloadCodec::Bytes => "bytes",
            salamander_db::PayloadCodec::Json => "json",
        },
    )?;
    let metadata = PyDict::new(py);
    for (key, value) in &row.metadata {
        metadata.set_item(key, PyBytes::new(py, value))?;
    }
    dict.set_item("metadata", metadata)?;
    dict.set_item("body", bytes_to_py(py, &row.payload)?)?;
    Ok(dict.into_any().unbind())
}

/// The user-facing stream (namespace) name stamped on a record, if any.
fn record_namespace(row: &RecordDto) -> Option<&str> {
    row.metadata
        .get(STREAM_NAME_KEY)
        .and_then(|value| std::str::from_utf8(value).ok())
}

fn rows_to_pylist(py: Python<'_>, rows: &[RecordDto]) -> PyResult<Py<PyList>> {
    let out = PyList::empty(py);
    for row in rows {
        out.append(row_to_py(py, row)?)?;
    }
    Ok(out.unbind())
}

fn branch_info_to_py(py: Python<'_>, info: &BranchDto) -> PyResult<PyObject> {
    let out = PyDict::new(py);
    out.set_item("id", hex_id(info.id))?;
    out.set_item("name", &info.name)?;
    out.set_item("parent_id", info.parent_id.map(hex_id))?;
    out.set_item("fork_position", info.fork_position)?;
    out.set_item("created_at_unix_nanos", info.created_at_unix_nanos)?;
    out.set_item("status", if info.archived { "archived" } else { "active" })?;
    let metadata = PyDict::new(py);
    for (key, value) in &info.metadata {
        metadata.set_item(key, PyBytes::new(py, value))?;
    }
    out.set_item("metadata", metadata)?;
    Ok(out.into_any().unbind())
}

fn receipt_to_py(
    py: Python<'_>,
    receipt: &salamander_db::EngineAppendReceipt,
) -> PyResult<PyObject> {
    let out = PyDict::new(py);
    out.set_item("batch_id", hex_id(receipt.batch_id))?;
    out.set_item("first_position", receipt.first_position)?;
    out.set_item("last_position", receipt.last_position)?;
    out.set_item("stream_id", hex_id(receipt.stream_id))?;
    out.set_item("previous_revision", receipt.previous_revision)?;
    out.set_item("current_revision", receipt.current_revision)?;
    out.set_item(
        "durability",
        match receipt.durability {
            DurabilityDto::Buffered => "buffered",
            DurabilityDto::Flush => "flushed",
            DurabilityDto::Sync => "synced",
        },
    )?;
    Ok(out.into_any().unbind())
}

fn snapshot_info_to_py(py: Python<'_>, info: &salamander_db::SnapshotInfo) -> PyResult<PyObject> {
    let out = PyDict::new(py);
    out.set_item("id", &info.id)?;
    out.set_item("projection", &info.manifest.projection_name)?;
    out.set_item("cursor", info.manifest.cursor.position)?;
    out.set_item("created_at_unix_nanos", info.manifest.created_at_unix_nanos)?;
    out.set_item("state_bytes", info.manifest.uncompressed_len)?;
    Ok(out.into_any().unbind())
}

fn bytes_to_py(py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| PyValueError::new_err(error.to_string()))?;
    value_to_py(py, &value)
}

fn bytes_rows_to_pylist(py: Python<'_>, rows: &[Vec<u8>]) -> PyResult<Py<PyList>> {
    let out = PyList::empty(py);
    for row in rows {
        out.append(bytes_to_py(py, row)?)?;
    }
    Ok(out.unbind())
}

fn value_bytes(value: &Value) -> PyResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| PyValueError::new_err(error.to_string()))
}

fn index_key_bytes(value: &Value) -> Vec<u8> {
    value.as_str().map_or_else(
        || value.to_string().into_bytes(),
        |value| value.as_bytes().to_vec(),
    )
}

fn hex_id(id: [u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn to_pyerr(error: EngineError) -> PyErr {
    match error.category {
        ErrorCategory::InvalidArgument => InvalidArgumentError::new_err(error.to_string()),
        ErrorCategory::Conflict => ConflictError::new_err(error.to_string()),
        ErrorCategory::NotFound => NotFoundError::new_err(error.to_string()),
        ErrorCategory::Locked => LockedError::new_err(error.to_string()),
        ErrorCategory::Io => IoError::new_err(error.to_string()),
        ErrorCategory::Corruption => CorruptionError::new_err(error.to_string()),
        ErrorCategory::UnsupportedFormat => UnsupportedFormatError::new_err(error.to_string()),
        ErrorCategory::Codec => CodecError::new_err(error.to_string()),
        ErrorCategory::Cancelled => CancelledError::new_err(error.to_string()),
        ErrorCategory::ResourceLimit => ResourceLimitError::new_err(error.to_string()),
        ErrorCategory::Internal => SalamanderError::new_err(error.to_string()),
    }
}

fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(value) = obj.downcast::<PyBool>() {
        return Ok(Value::Bool(value.is_true()));
    }
    if let Ok(value) = obj.extract::<i64>() {
        return Ok(Value::from(value));
    }
    if let Ok(value) = obj.extract::<f64>() {
        return Ok(Value::from(value));
    }
    if let Ok(value) = obj.extract::<String>() {
        return Ok(Value::String(value));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        return Ok(Value::Array(
            list.iter()
                .map(|item| py_to_value(&item))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            map.insert(key.extract()?, py_to_value(&value)?);
        }
        return Ok(Value::Object(map));
    }
    Err(PyValueError::new_err(format!(
        "object of type {} is not JSON-serializable",
        obj.get_type().name()?
    )))
}

fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    Ok(match value {
        Value::Null => py.None(),
        Value::Bool(value) => value.into_pyobject(py)?.to_owned().into_any().unbind(),
        Value::Number(value) if value.is_i64() => value
            .as_i64()
            .unwrap()
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        Value::Number(value) if value.is_u64() => value
            .as_u64()
            .unwrap()
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        Value::Number(value) => value
            .as_f64()
            .unwrap_or(f64::NAN)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        Value::String(value) => value.into_pyobject(py)?.into_any().unbind(),
        Value::Array(values) => {
            let out = PyList::empty(py);
            for value in values {
                out.append(value_to_py(py, value)?)?;
            }
            out.into_any().unbind()
        }
        Value::Object(values) => {
            let out = PyDict::new(py);
            for (key, value) in values {
                out.set_item(key, value_to_py(py, value)?)?;
            }
            out.into_any().unbind()
        }
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (path, commit_every_bytes=None, commit_every_count=None, commit_every_millis=None, snapshot_every_events=None, snapshot_every_bytes=None, snapshot_every_millis=None))]
fn open(
    py: Python<'_>,
    path: &str,
    commit_every_bytes: Option<u64>,
    commit_every_count: Option<u64>,
    commit_every_millis: Option<u64>,
    snapshot_every_events: Option<u64>,
    snapshot_every_bytes: Option<u64>,
    snapshot_every_millis: Option<u64>,
) -> PyResult<Salamander> {
    Salamander::open(
        py,
        path,
        commit_every_bytes,
        commit_every_count,
        commit_every_millis,
        snapshot_every_events,
        snapshot_every_bytes,
        snapshot_every_millis,
    )
}

#[pymodule]
fn salamander(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Salamander>()?;
    m.add_class::<View>()?;
    m.add_class::<Reader>()?;
    m.add_class::<Watch>()?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add("SalamanderError", m.py().get_type::<SalamanderError>())?;
    m.add(
        "InvalidArgumentError",
        m.py().get_type::<InvalidArgumentError>(),
    )?;
    m.add("ConflictError", m.py().get_type::<ConflictError>())?;
    m.add("NotFoundError", m.py().get_type::<NotFoundError>())?;
    m.add("LockedError", m.py().get_type::<LockedError>())?;
    m.add("IoError", m.py().get_type::<IoError>())?;
    m.add("CorruptionError", m.py().get_type::<CorruptionError>())?;
    m.add(
        "UnsupportedFormatError",
        m.py().get_type::<UnsupportedFormatError>(),
    )?;
    m.add("CodecError", m.py().get_type::<CodecError>())?;
    m.add(
        "ResourceLimitError",
        m.py().get_type::<ResourceLimitError>(),
    )?;
    m.add("CancelledError", m.py().get_type::<CancelledError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
