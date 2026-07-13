//! docs/phase-1.5.md §3 — `IndexedView`: the ready-made
//! queryable view. A primary `BTreeMap` store plus named secondary
//! indexes, all maintained incrementally as events fan in. The `indexed_as`
//! reverse map (§5) is the correctness core: it's what lets an update or
//! delete remove the *old* value's index entries before adding the new
//! ones, so `by(index, key)` never returns a phantom hit.

use std::any::Any;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::ops::RangeBounds;

use super::{IndexKey, View};
use crate::event::{Body, Event};
use crate::projection::Projection;

/// The change a `project` closure derives from one event: upsert or remove
/// a primary key. Events irrelevant to this view yield `None` instead.
pub enum Change<K, V> {
    /// Insert or replace the value at the primary key `K`.
    Put(K, V),
    /// Remove the entry at the primary key `K`.
    Delete(K),
}

impl<K, V> Change<K, V> {
    /// Constructs a [`Change::Put`].
    pub fn put(key: K, value: V) -> Self {
        Change::Put(key, value)
    }
    /// Constructs a [`Change::Delete`].
    pub fn delete(key: K) -> Self {
        Change::Delete(key)
    }
}

// Boxed closures, aliased so the struct fields don't trip `type_complexity`
// and read as intent rather than punctuation.
type Projector<K, V, B> = Box<dyn Fn(&Event<B>) -> Option<Change<K, V>>>;
type Indexer<V> = Box<dyn Fn(&V) -> Vec<IndexKey>>;

/// A live, queryable view over payloads of type `B`, keyed by `K` with
/// values `V`. `B` is the third type parameter because the `project`
/// closure matches on `&Event<B>` — the query-layer generalization of
/// query-layer design §9 OQ-Q4 (the agent `EventBody` is just one `B`).
pub struct IndexedView<K, V, B> {
    project: Projector<K, V, B>,
    indexers: Vec<(String, Indexer<V>)>,
    primary: BTreeMap<K, V>,
    indexes: HashMap<String, BTreeMap<IndexKey, BTreeSet<K>>>,
    /// Reverse map: for each present primary key, the `(index, index_key)`
    /// pairs it currently contributes. The §5 update/delete correctness core.
    indexed_as: HashMap<K, Vec<(String, IndexKey)>>,
    cursor: u64,
}

/// Builder for [`IndexedView`] — `.project(..)` (required) then any number
/// of `.index(name, ..)`, then `.build()`.
/// Builder for an [`IndexedView`]: supply a `project` closure and any
/// number of named secondary indexes, then `build`.
pub struct IndexedViewBuilder<K, V, B> {
    project: Option<Projector<K, V, B>>,
    indexers: Vec<(String, Indexer<V>)>,
}

impl<K, V, B> IndexedView<K, V, B> {
    /// Starts building a view; call `project` (required) and `index`
    /// (optional) on the returned builder.
    pub fn builder() -> IndexedViewBuilder<K, V, B> {
        IndexedViewBuilder {
            project: None,
            indexers: Vec::new(),
        }
    }
}

impl<K, V, B> IndexedViewBuilder<K, V, B> {
    /// The projection function: map one event to an optional primary-key
    /// change. This is where the caller's payload vocabulary is decoded.
    pub fn project(mut self, f: impl Fn(&Event<B>) -> Option<Change<K, V>> + 'static) -> Self {
        self.project = Some(Box::new(f));
        self
    }

    /// Add a named secondary index: map a value to zero or more index keys.
    /// A value may appear under several keys (multi-valued index) and
    /// several values may share a key.
    pub fn index(mut self, name: &str, f: impl Fn(&V) -> Vec<IndexKey> + 'static) -> Self {
        self.indexers.push((name.to_string(), Box::new(f)));
        self
    }

    /// Finishes the view. Panics if no `project` closure was supplied.
    pub fn build(self) -> IndexedView<K, V, B> {
        IndexedView {
            project: self
                .project
                .expect("IndexedView::build requires a .project() closure"),
            indexers: self.indexers,
            primary: BTreeMap::new(),
            indexes: HashMap::new(),
            indexed_as: HashMap::new(),
            cursor: 0,
        }
    }
}

// ── incremental maintenance (the fold) ───────────────────────────────────

impl<K, V, B> IndexedView<K, V, B>
where
    K: Ord + Hash + Clone,
{
    /// Apply one event: run the projector, fold any resulting change into
    /// the primary store and every index, then advance the cursor. Shared
    /// by both the `View` and `Projection` `apply` impls so live fan-out
    /// and on-demand replay produce byte-identical state.
    fn record(&mut self, event: &Event<B>) {
        if let Some(change) = (self.project)(event) {
            match change {
                Change::Put(k, v) => self.put(k, v),
                Change::Delete(k) => self.delete(&k),
            }
        }
        self.cursor = event.offset + 1;
    }

    fn put(&mut self, key: K, value: V) {
        // Clear the previous value's index entries first (§5): an update
        // from V1 to V2 must not leave V1's keys behind.
        if let Some(stale) = self.indexed_as.remove(&key) {
            self.prune_index_entries(&key, &stale);
        }

        let mut recorded: Vec<(String, IndexKey)> = Vec::new();
        for (name, indexer) in &self.indexers {
            for index_key in indexer(&value) {
                recorded.push((name.clone(), index_key));
            }
        }
        for (name, index_key) in &recorded {
            self.indexes
                .entry(name.clone())
                .or_default()
                .entry(index_key.clone())
                .or_default()
                .insert(key.clone());
        }

        self.indexed_as.insert(key.clone(), recorded);
        self.primary.insert(key, value);
    }

    fn delete(&mut self, key: &K) {
        if let Some(stale) = self.indexed_as.remove(key) {
            self.prune_index_entries(key, &stale);
        }
        self.primary.remove(key);
    }

    /// Remove `key` from each `(index, index_key)` bucket in `entries`,
    /// dropping buckets that become empty so `by` never scans dead keys.
    fn prune_index_entries(&mut self, key: &K, entries: &[(String, IndexKey)]) {
        for (name, index_key) in entries {
            if let Some(index) = self.indexes.get_mut(name) {
                if let Some(set) = index.get_mut(index_key) {
                    set.remove(key);
                    if set.is_empty() {
                        index.remove(index_key);
                    }
                }
            }
        }
    }
}

// ── the query surface (reached after downcast) ───────────────────────────

impl<K, V, B> IndexedView<K, V, B>
where
    K: Ord,
{
    /// Point lookup by primary key.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.primary.get(key)
    }

    /// Ordered scan over a primary-key range (keys are `Ord`, stored in a
    /// `BTreeMap`).
    pub fn range<T, R>(&self, range: R) -> impl Iterator<Item = (&K, &V)>
    where
        T: Ord + ?Sized,
        K: Borrow<T>,
        R: RangeBounds<T>,
    {
        self.primary.range(range)
    }

    /// Secondary-index lookup: every value whose *current* row maps to
    /// `index_key` under `index`, in primary-key order. Returns empty for
    /// an unknown index or a key with no live entries.
    pub fn by(&self, index: &str, index_key: &[u8]) -> Vec<&V> {
        self.indexes
            .get(index)
            .and_then(|idx| idx.get(index_key))
            .into_iter()
            .flatten()
            .filter_map(|k| self.primary.get(k))
            .collect()
    }

    /// Number of primary entries in the view.
    pub fn len(&self) -> usize {
        self.primary.len()
    }

    /// Whether the view has no primary entries.
    pub fn is_empty(&self) -> bool {
        self.primary.is_empty()
    }
}

/// `prefix` is a range special case, and only well-defined for keys with a
/// notion of "starts with" — so it's offered for the two natural key types
/// (`String` here, `Vec<u8>` below) rather than all `K: Ord`.
impl<V, B> IndexedView<String, V, B> {
    /// Every entry whose key starts with `prefix`, in key order. Relies on
    /// prefixed keys forming a contiguous block in sorted order.
    pub fn prefix<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = (&'a String, &'a V)> + 'a {
        self.primary
            .range(prefix.to_string()..)
            .take_while(move |(k, _)| k.starts_with(prefix))
    }
}

impl<V, B> IndexedView<Vec<u8>, V, B> {
    /// Every entry whose byte key starts with `prefix`, in key order.
    pub fn prefix<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a V)> + 'a {
        self.primary
            .range(prefix.to_vec()..)
            .take_while(move |(k, _)| k.starts_with(prefix))
    }
}

// ── trait impls: driven by the registry (View) and by replay (Projection) ─

impl<K, V, B> View<B> for IndexedView<K, V, B>
where
    K: Ord + Hash + Clone + 'static,
    V: 'static,
    B: 'static,
{
    fn apply(&mut self, event: &Event<B>) {
        self.record(event);
    }
    fn cursor(&self) -> u64 {
        self.cursor
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<K, V, B> Projection for IndexedView<K, V, B>
where
    K: Ord + Hash + Clone + 'static,
    V: 'static,
    B: Body,
{
    type Body = B;
    type State = BTreeMap<K, V>;

    fn apply(&mut self, event: &Event<B>) {
        self.record(event);
    }
    fn cursor(&self) -> u64 {
        self.cursor
    }
    fn state(&self) -> &Self::State {
        &self.primary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A tiny non-serde payload — the index logic doesn't need `Body`, only
    // the `View`/`replay` boundary does, so these tests exercise the fold
    // directly through `record`.
    #[derive(Clone)]
    enum Ev {
        Set(&'static str, u32),
        Del(&'static str),
    }

    fn ev(offset: u64, body: Ev) -> Event<Ev> {
        Event {
            offset,
            timestamp_ms: 0,
            namespace: "ns".to_string(),
            body,
        }
    }

    fn view() -> IndexedView<String, u32, Ev> {
        IndexedView::builder()
            .project(|e: &Event<Ev>| match &e.body {
                Ev::Set(k, v) => Some(Change::put(k.to_string(), *v)),
                Ev::Del(k) => Some(Change::delete(k.to_string())),
            })
            .index("by_val", |v: &u32| vec![v.to_le_bytes().to_vec()])
            .build()
    }

    fn k(n: u32) -> Vec<u8> {
        n.to_le_bytes().to_vec()
    }

    #[test]
    fn get_and_range() {
        let mut v = view();
        v.record(&ev(0, Ev::Set("a", 1)));
        v.record(&ev(1, Ev::Set("b", 2)));
        v.record(&ev(2, Ev::Set("c", 3)));

        assert_eq!(v.get("a"), Some(&1));
        assert_eq!(v.get("z"), None);
        assert_eq!(v.len(), 3);
        assert_eq!(v.cursor(), 3);

        let scan: Vec<_> = v
            .range("a".to_string().."c".to_string())
            .map(|(k, val)| (k.clone(), *val))
            .collect();
        assert_eq!(scan, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    }

    #[test]
    fn prefix_scan() {
        let mut v = view();
        v.record(&ev(0, Ev::Set("tool:a", 1)));
        v.record(&ev(1, Ev::Set("tool:b", 2)));
        v.record(&ev(2, Ev::Set("user:c", 3)));

        let hits: Vec<_> = v.prefix("tool:").map(|(k, _)| k.clone()).collect();
        assert_eq!(hits, vec!["tool:a".to_string(), "tool:b".to_string()]);
    }

    #[test]
    fn by_returns_all_keys_sharing_an_index_key() {
        let mut v = view();
        v.record(&ev(0, Ev::Set("a", 1)));
        v.record(&ev(1, Ev::Set("b", 1)));
        v.record(&ev(2, Ev::Set("c", 2)));

        // a and b both map to value 1, in primary-key order.
        assert_eq!(v.by("by_val", &k(1)), vec![&1, &1]);
        assert_eq!(v.by("by_val", &k(2)), vec![&2]);
        assert!(v.by("by_val", &k(9)).is_empty());
        assert!(v.by("no_such_index", &k(1)).is_empty());
    }

    #[test]
    fn update_moves_the_secondary_index_entry() {
        let mut v = view();
        v.record(&ev(0, Ev::Set("a", 1)));
        assert_eq!(v.by("by_val", &k(1)), vec![&1]);

        // Update a: 1 -> 5. The old index entry must vanish, the new appear.
        v.record(&ev(1, Ev::Set("a", 5)));
        assert!(
            v.by("by_val", &k(1)).is_empty(),
            "stale index entry left behind"
        );
        assert_eq!(v.by("by_val", &k(5)), vec![&5]);
        assert_eq!(v.get("a"), Some(&5));
    }

    #[test]
    fn delete_removes_primary_and_all_index_entries() {
        let mut v = view();
        v.record(&ev(0, Ev::Set("a", 1)));
        v.record(&ev(1, Ev::Set("b", 1)));
        v.record(&ev(2, Ev::Del("a")));

        assert_eq!(v.get("a"), None);
        assert_eq!(v.by("by_val", &k(1)), vec![&1]); // only b remains

        v.record(&ev(3, Ev::Del("b")));
        assert!(v.by("by_val", &k(1)).is_empty());

        // The now-empty index bucket must be pruned, and the reverse map
        // must be empty — no orphaned bookkeeping (§5).
        assert!(!v.indexes["by_val"].contains_key(&k(1)));
        assert!(v.indexed_as.is_empty());
    }

    #[test]
    fn churn_leaves_no_orphaned_bookkeeping() {
        let mut v = view();
        // put -> update -> update -> delete, twice, interleaved.
        v.record(&ev(0, Ev::Set("a", 1)));
        v.record(&ev(1, Ev::Set("a", 2)));
        v.record(&ev(2, Ev::Set("b", 2)));
        v.record(&ev(3, Ev::Set("a", 3)));
        v.record(&ev(4, Ev::Del("a")));
        v.record(&ev(5, Ev::Del("b")));

        assert!(v.is_empty());
        assert!(v.indexed_as.is_empty());
        // Every index bucket should have been pruned as it emptied.
        assert!(v.indexes.values().all(|idx| idx.is_empty()));
    }
}
