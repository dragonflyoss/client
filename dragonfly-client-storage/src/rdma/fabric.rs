/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Safe wrapper around the libfabric C shim (shim.c).
//!
//! One [`Fabric`] wraps one shared FI_EP_RDM endpoint. Transfers multiplex over it with
//! unique tags; per-operation completions are routed by a single progress thread that polls
//! the completion queue.
//!
//! Threading model: the shim requires providers to grant `FI_THREAD_SAFE`, allowing posts,
//! memory registration, and CQ progress to run concurrently. Cancellation and CQ reads retain
//! a narrow lock solely to protect the operation-context lifetime race between those paths.
//!
//! Buffer lifetime invariant: the NIC may DMA into a posted buffer until the completion
//! (success, error, or FI_ECANCELED after fi_cancel) is reaped. Every posted operation
//! therefore holds an `Arc<PinnedBuf>` in the pending-operation map, and the map entry is
//! only removed by the progress thread when the completion arrives. A buffer whose
//! operation never completes is intentionally leaked rather than freed under the NIC.

use dragonfly_client_core::{Error, Result};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tracing::{error, info, warn};

/// ffi declares the C ABI exported by shim.c.
mod ffi {
    use std::ffi::{c_char, c_int, c_void};

    /// DfrdmaFabric is the opaque fabric handle defined by shim.c.
    #[repr(C)]
    pub struct DfrdmaFabric {
        _private: [u8; 0],
    }

    /// DfrdmaCompletion mirrors the batched CQ result structure in shim.c.
    #[derive(Clone, Copy)]
    #[repr(C)]
    pub struct DfrdmaCompletion {
        pub context: *mut c_void,
        pub flags: u64,
        pub len: usize,
        pub err: i64,
    }

    extern "C" {
        pub fn dfrdma_open(
            prov_name: *const c_char,
            domain_name: *const c_char,
            out: *mut *mut DfrdmaFabric,
        ) -> c_int;
        pub fn dfrdma_close(f: *mut DfrdmaFabric);
        pub fn dfrdma_close_endpoint(f: *mut DfrdmaFabric) -> c_int;
        pub fn dfrdma_provider_name(f: *mut DfrdmaFabric) -> *const c_char;
        pub fn dfrdma_max_msg_size(f: *mut DfrdmaFabric) -> usize;
        pub fn dfrdma_mr_required(f: *mut DfrdmaFabric) -> c_int;
        pub fn dfrdma_strerror(err: i64) -> *const c_char;
        pub fn dfrdma_getname(f: *mut DfrdmaFabric, buf: *mut u8, len: *mut usize) -> c_int;
        pub fn dfrdma_av_insert(
            f: *mut DfrdmaFabric,
            addr: *const u8,
            len: usize,
            out: *mut u64,
        ) -> c_int;
        pub fn dfrdma_mr_reg(
            f: *mut DfrdmaFabric,
            buf: *mut c_void,
            len: usize,
            mr_out: *mut *mut c_void,
            desc_out: *mut *mut c_void,
        ) -> c_int;
        pub fn dfrdma_mr_close(mr: *mut c_void) -> c_int;
        pub fn dfrdma_trecv(
            f: *mut DfrdmaFabric,
            buf: *mut c_void,
            len: usize,
            desc: *mut c_void,
            tag: u64,
            context: *mut c_void,
        ) -> i64;
        pub fn dfrdma_tsend(
            f: *mut DfrdmaFabric,
            buf: *const c_void,
            len: usize,
            desc: *mut c_void,
            dest: u64,
            tag: u64,
            context: *mut c_void,
        ) -> i64;
        pub fn dfrdma_cq_read_batch(
            f: *mut DfrdmaFabric,
            out: *mut DfrdmaCompletion,
            capacity: usize,
        ) -> c_int;
        pub fn dfrdma_cancel(f: *mut DfrdmaFabric, context: *mut c_void) -> c_int;
    }
}

/// BUDGET_UNIT is the granularity of the registered-memory budget semaphore.
const BUDGET_UNIT: u64 = 64 * 1024;

/// TAG_RANGE_SIZE reserves one disjoint tag block per transfer. A transfer uses its base
/// tag plus at most TAG_RANGE_SIZE - 1 chunk indices.
pub(crate) const TAG_RANGE_SIZE: u64 = 4096;

/// MAX_RESOLVED_PEERS bounds provider address-vector and process memory growth caused by
/// resolving a stream of unique endpoint addresses.
const MAX_RESOLVED_PEERS: usize = 65_536;

/// POST_RETRY_INTERVAL is the pause between retries when a transmit or receive queue is
/// full (FI_EAGAIN).
const POST_RETRY_INTERVAL: Duration = Duration::from_micros(200);

/// POST_RETRY_TIMEOUT bounds how long a single post is retried before giving up.
const POST_RETRY_TIMEOUT: Duration = Duration::from_secs(5);

/// PROGRESS_IDLE_INTERVAL is the sleep between completion-queue polls when idle.
const PROGRESS_IDLE_INTERVAL: Duration = Duration::from_micros(100);

/// PROGRESS_ACTIVE_INTERVAL bounds CPU use after a burst of active-operation yields.
const PROGRESS_ACTIVE_INTERVAL: Duration = Duration::from_micros(10);

/// PROGRESS_ACTIVE_YIELDS is the number of scheduler yields before a short active sleep.
const PROGRESS_ACTIVE_YIELDS: u32 = 64;

/// CQ_BATCH_SIZE amortizes the FFI, cancellation lock, and pending-map lock across completions.
const CQ_BATCH_SIZE: usize = 32;

/// CANCEL_GRACE_TIMEOUT is how long a timed-out operation waits for its cancellation
/// completion before its buffer is intentionally leaked.
const CANCEL_GRACE_TIMEOUT: Duration = Duration::from_secs(5);

/// GETNAME_INITIAL_CAPACITY is the initial buffer size for fi_getname; provider endpoint
/// addresses are typically well under this.
const GETNAME_INITIAL_CAPACITY: usize = 512;

/// fi_error converts a negative fi_errno value into a client error with the libfabric
/// error string.
fn fi_error(op: &str, rc: i64) -> Error {
    // Safety: dfrdma_strerror always returns a valid static string.
    let message = unsafe { CStr::from_ptr(ffi::dfrdma_strerror(rc)) };
    Error::Unknown(format!(
        "libfabric {} failed: {} ({})",
        op,
        message.to_string_lossy(),
        rc
    ))
}

/// Completion is the result of one posted operation.
#[derive(Debug, Clone, Copy)]
struct Completion {
    /// len is the number of bytes transferred.
    len: usize,

    /// err is 0 on success or a positive fi_errno value.
    err: i64,
}

/// CtxBlock is per-operation scratch space handed to libfabric as the operation context.
/// Providers that require the FI_CONTEXT/FI_CONTEXT2 mode bits write into it, so it must
/// stay allocated until the completion is reaped. 128 bytes covers fi_context2 (64 bytes)
/// with slack.
#[repr(C, align(16))]
struct CtxBlock([u8; 128]);

/// PendingOp tracks one posted operation until its completion arrives.
struct PendingOp {
    /// id distinguishes this operation from a later one that reuses its context address. The
    /// allocator is free to hand the same block to the next post once a completion is reaped,
    /// so the address alone cannot identify an operation.
    id: u64,

    /// tx delivers the completion to the waiting task.
    tx: oneshot::Sender<Completion>,

    /// _ctx keeps the provider scratch space alive for the duration of the operation.
    _ctx: Box<CtxBlock>,

    /// _buf keeps the posted buffer (and its memory registration) alive until the hardware
    /// is done with it.
    _buf: Arc<PinnedBuf>,
}

/// Handle owns the raw fabric pointer. Endpoint calls take a shared lifecycle lock, while
/// failure recovery takes the exclusive lock to close the endpoint before pending buffers
/// are released. The remaining domain objects stay alive until the last registered buffer drops.
struct Handle {
    raw: *mut ffi::DfrdmaFabric,
    endpoint_lifecycle: RwLock<()>,
    endpoint_open: AtomicBool,

    /// endpoint_close_drains records whether closing the endpoint is known to stop the device
    /// from writing into buffers that were posted to it. See [`endpoint_close_drains`].
    endpoint_close_drains: bool,
}

/// endpoint_close_drains reports whether `fi_close` on an endpoint of this provider is a hardware
/// barrier: after it returns, nothing can still land in a buffer that was posted to that endpoint.
///
/// libfabric does not promise this in general. It asks the application to complete or cancel every
/// operation before closing, and leaves the behaviour of closing with work outstanding to the
/// provider. The abort path needs the opposite guarantee, because it runs precisely when an
/// operation could not be cancelled, so it has to know the answer per provider rather than assume
/// one:
///
///   - `efa` and `verbs` close the endpoint by destroying the underlying queue pair. The kernel
///     driver takes the queue pair out of service before `ibv_destroy_qp` returns, so the NIC can
///     no longer reach the posted buffers.
///   - `tcp`, `udp`, `sockets` and `shm` are software providers. No device DMA is involved, and
///     closing the endpoint tears down the socket the progress thread was using.
///
/// Anything else is treated as unknown, and the abort path quarantines the buffers instead of
/// freeing them. That leaks the in-flight registrations for the life of the process, which is the
/// correct trade against handing memory back to the allocator while a device may still write it.
fn endpoint_close_drains(provider: &str) -> bool {
    // Providers are reported as either a bare name or "base;layered", e.g. "verbs;ofi_rxm".
    let base = provider.split(';').next().unwrap_or(provider);
    matches!(base, "efa" | "verbs" | "tcp" | "udp" | "sockets" | "shm")
}

/// Safety: the shim rejects providers that do not grant FI_THREAD_SAFE, and the handle is
/// closed only after the progress thread exits.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        // Safety: the pointer came from dfrdma_open and is dropped exactly once. Registered
        // buffers hold an Arc<Handle>, so their MRs close before this last owner disappears.
        unsafe { ffi::dfrdma_close(self.raw) };
    }
}

impl Handle {
    /// close_endpoint prevents further DMA before failure recovery releases pending buffers.
    fn close_endpoint(&self) -> bool {
        let _lifecycle = self.endpoint_lifecycle.write().unwrap();
        if !self.endpoint_open.load(Ordering::Acquire) {
            return true;
        }
        // Safety: raw remains valid for the lifetime of Handle. The exclusive lifecycle lock
        // excludes posts, cancellation, and CQ reads while fi_close operates on the endpoint.
        let rc = unsafe { ffi::dfrdma_close_endpoint(self.raw) };
        if rc == 0 {
            self.endpoint_open.store(false, Ordering::Release);
            true
        } else {
            error!("failed to close rdma endpoint during recovery: {}", rc);
            false
        }
    }
}

/// FabricInner is shared by the fabric API, the progress thread, and pinned buffers.
struct FabricInner {
    /// cancel_progress_lock serializes cancellation with completion reaping so a context
    /// cannot be freed between the pending-map check and fi_cancel. Hot-path posts do not
    /// take this lock.
    cancel_progress_lock: Mutex<()>,

    /// handle owns the fabric objects independently from FabricInner. Pinned buffers retain this
    /// handle directly, avoiding a pending -> buffer -> FabricInner ownership cycle.
    handle: Arc<Handle>,

    /// pending maps context addresses to in-flight operations.
    pending: Mutex<HashMap<usize, PendingOp>>,

    /// op_counter issues the identifiers that tell a live operation apart from a completed one
    /// whose context address has been reused.
    op_counter: AtomicU64,

    /// av maps peer endpoint addresses to fabric addresses (fi_addr_t).
    av: Mutex<HashMap<Vec<u8>, u64>>,

    /// shutdown stops the progress thread.
    shutdown: AtomicBool,

    /// failed rejects new work after an unrecoverable operation or CQ failure.
    failed: AtomicBool,

    /// failure_reason records the first fatal error for callers and diagnostics.
    failure_reason: Mutex<Option<String>>,

    /// failure_notify wakes buffer waiters when the endpoint is retired.
    failure_notify: Notify,

    /// mr_required is true when the provider needs local buffers registered.
    mr_required: bool,
}

impl FabricInner {
    /// failure returns the first fatal fabric error.
    fn failure(&self) -> Option<String> {
        self.failure_reason.lock().unwrap().clone()
    }

    /// ensure_healthy rejects work after the endpoint has been retired.
    fn ensure_healthy(&self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::Unknown(
                self.failure()
                    .unwrap_or_else(|| "rdma fabric is unavailable".to_string()),
            ));
        }
        Ok(())
    }

    /// fail_and_abort permanently retires this endpoint. Pending buffers are released only
    /// after endpoint close succeeds; otherwise they remain quarantined for memory safety.
    fn fail_and_abort(&self, reason: String) {
        if self
            .failed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        *self.failure_reason.lock().unwrap() = Some(reason.clone());
        error!("retiring rdma fabric: {}", reason);

        let _progress_guard = self.cancel_progress_lock.lock().unwrap();
        let closed = self.handle.close_endpoint();
        if closed && self.handle.endpoint_close_drains {
            // Closing the endpoint is the supported way to abort posted receives on this
            // provider, and it has returned, so nothing can reach these buffers any more.
            self.pending.lock().unwrap().clear();
        } else if closed {
            error!(
                "rdma endpoint closed but this provider does not guarantee the device has stopped; \
                 pending buffers remain quarantined for process lifetime"
            );
        } else {
            error!(
                "rdma endpoint close failed; pending buffers remain quarantined for process lifetime"
            );
        }
        self.failure_notify.notify_waiters();
    }

    /// cancel_ctx attempts to cancel an operation that is still tracked. CQ progress cannot
    /// remove and free the context between this lookup and fi_cancel. `id` is checked because a
    /// completion may already have been reaped and the context address handed to a different
    /// operation, which must not be cancelled in this one's place.
    fn cancel_ctx(&self, ctx_addr: usize, id: u64) {
        let cancel_error = {
            let _progress_guard = self.cancel_progress_lock.lock().unwrap();
            if self.failed.load(Ordering::Acquire) || !self.is_pending(ctx_addr, id) {
                None
            } else {
                let _lifecycle = self.handle.endpoint_lifecycle.read().unwrap();
                if !self.handle.endpoint_open.load(Ordering::Acquire) {
                    None
                } else {
                    // Safety: the pending entry owns the context block. The shim normalizes
                    // an already-completed operation to success; other failures are fatal
                    // because the provider may continue to access the buffer.
                    let rc =
                        unsafe { ffi::dfrdma_cancel(self.handle.raw, ctx_addr as *mut c_void) };
                    (rc != 0).then_some(rc)
                }
            }
        };

        if let Some(rc) = cancel_error {
            self.fail_and_abort(format!("rdma operation cancellation failed: {rc}"));
        }
    }

    /// is_pending reports whether the operation identified by both its context address and its id
    /// is still in flight.
    fn is_pending(&self, ctx_addr: usize, id: u64) -> bool {
        self.pending
            .lock()
            .unwrap()
            .get(&ctx_addr)
            .is_some_and(|op| op.id == id)
    }
}

impl Drop for FabricInner {
    fn drop(&mut self) {
        let pending = std::mem::take(self.pending.get_mut().unwrap());
        if !pending.is_empty() {
            // A shutdown that reached the provider empties this map, so anything left here is an
            // operation the provider never gave back: either the endpoint would not close or the
            // provider does not promise the device stops when it does. Leaking the map also
            // retains Handle, and is safer than returning memory a device may still write.
            error!(
                "leaking {} in-flight rdma registrations that the provider never released",
                pending.len()
            );
            std::mem::forget(pending);
        }
    }
}

/// MrGuard closes a memory registration when dropped. It is a separate struct from
/// PinnedBuf so that field drop order guarantees the registration is closed before the
/// buffer memory is freed.
struct MrGuard {
    /// mr is the raw memory-region handle, null when the buffer is unregistered.
    mr: *mut c_void,

    /// handle keeps the domain alive until this registration has closed.
    _handle: Arc<Handle>,
}

/// Safety: the owning fabric requires FI_THREAD_SAFE and closes the registration before freeing it.
unsafe impl Send for MrGuard {}
unsafe impl Sync for MrGuard {}

impl Drop for MrGuard {
    fn drop(&mut self) {
        if self.mr.is_null() {
            return;
        }
        // Safety: mr came from dfrdma_mr_reg and is closed exactly once. Handle remains alive
        // through this guard, and endpoint teardown deliberately leaves the domain open.
        let rc = unsafe { ffi::dfrdma_mr_close(self.mr) };
        if rc != 0 {
            warn!("failed to close rdma memory region: {}", rc);
        }
    }
}

/// PinnedBuf is a fixed transfer buffer, optionally registered with the NIC, accounted
/// against the fabric's registered-memory budget.
///
/// The NIC writes into the buffer while operations are in flight, so the data is behind an
/// UnsafeCell and must only be accessed through [`PinnedBuf::as_mut_slice`] before posting
/// or after all completions have been reaped.
pub struct PinnedBuf {
    /// mr_guard closes the registration before data is freed (field order matters).
    mr_guard: MrGuard,

    /// desc is the local descriptor passed to post calls, null when unregistered.
    desc: *mut c_void,

    /// data is the buffer itself; the Vec is never resized so its pointer is stable.
    data: UnsafeCell<Vec<u8>>,

    /// _permit returns the buffer's bytes to the registered-memory budget on drop.
    _permit: OwnedSemaphorePermit,
}

/// Safety: concurrent access is limited to the NIC DMA-ing into disjoint posted ranges;
/// the CPU only touches the data before posting or after completions.
unsafe impl Send for PinnedBuf {}
unsafe impl Sync for PinnedBuf {}

impl PinnedBuf {
    /// len returns the buffer length in bytes.
    pub fn len(&self) -> usize {
        // Safety: the Vec header is only mutated at construction.
        unsafe { (*self.data.get()).len() }
    }

    /// is_empty returns whether the buffer is zero-length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// as_mut_slice exposes the buffer for filling or reading.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no operation over this buffer is currently posted.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn as_mut_slice(&self) -> &mut [u8] {
        (*self.data.get()).as_mut_slice()
    }

    /// as_slice exposes the buffer for reading. Readers must use this rather than
    /// [`PinnedBuf::as_mut_slice`]: two concurrent readers of one buffer would otherwise each
    /// materialize a `&mut [u8]` over the same bytes, which aliases.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no operation over this buffer is currently posted and that
    /// nothing mutates it while the returned slice lives.
    pub unsafe fn as_slice(&self) -> &[u8] {
        (*self.data.get()).as_slice()
    }

    /// as_mut_range exposes one mutable subrange for pipelined filling.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no fabric operation or other CPU reference currently accesses
    /// any byte in `range`.
    #[allow(clippy::mut_from_ref)]
    unsafe fn as_mut_range(&self, range: std::ops::Range<usize>) -> &mut [u8] {
        &mut (*self.data.get()).as_mut_slice()[range]
    }

    /// ptr returns a raw pointer to the byte at `offset`.
    fn ptr(&self, offset: usize) -> *mut u8 {
        // Safety: offset is validated by the posting functions.
        unsafe { (*self.data.get()).as_mut_ptr().add(offset) }
    }

    /// into_vec extracts the buffer contents. When this Arc is the last reference (no
    /// operations in flight) the data is moved out without copying.
    pub fn into_vec(self: Arc<Self>) -> Vec<u8> {
        match Arc::try_unwrap(self) {
            Ok(buf) => {
                let PinnedBuf {
                    mr_guard,
                    data,
                    _permit,
                    ..
                } = buf;
                // Close the registration before handing out the memory.
                drop(mr_guard);
                data.into_inner()
            }
            Err(buf) => {
                warn!("rdma buffer still referenced, copying contents");
                // Safety: callers only convert after all completions were reaped.
                unsafe { (*buf.data.get()).clone() }
            }
        }
    }
}

/// BufferPoolStats is a snapshot of registered-buffer cache activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolStats {
    /// hits is the number of checkouts served by an existing registration.
    pub hits: u64,

    /// misses is the number of checkouts that allocated and registered a new buffer.
    pub misses: u64,

    /// cached_buffers is the number of idle registered buffers.
    pub cached_buffers: usize,

    /// cached_bytes is the total capacity of idle registered buffers.
    pub cached_bytes: usize,
}

/// BufferPool retains completed registered buffers for best-fit reuse. Cached buffers keep
/// their semaphore permits, so active plus idle memory remains bounded by the fabric budget.
struct BufferPool {
    /// idle contains buffers with no in-flight operation or reader.
    idle: Mutex<Vec<Arc<PinnedBuf>>>,

    /// changed wakes checkouts when a buffer is returned to the idle set.
    changed: Notify,

    /// closed prevents buffers from being retained after Fabric shutdown.
    closed: AtomicBool,

    /// hits counts successful idle-buffer reuse.
    hits: AtomicU64,

    /// misses counts new allocations and registrations.
    misses: AtomicU64,
}

impl BufferPool {
    /// new creates an empty registered-buffer pool.
    fn new() -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            changed: Notify::new(),
            closed: AtomicBool::new(false),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// take_best_fit removes the smallest idle buffer that can contain `len`. If none fits,
    /// all undersized buffers are evicted so their permits can satisfy a larger allocation.
    fn take_best_fit(&self, len: usize) -> Option<Arc<PinnedBuf>> {
        let evicted = {
            let mut idle = self.idle.lock().unwrap();
            let best = idle
                .iter()
                .enumerate()
                .filter(|(_, buf)| buf.len() >= len)
                .min_by_key(|(_, buf)| buf.len())
                .map(|(index, _)| index);
            if let Some(index) = best {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(idle.swap_remove(index));
            }
            std::mem::take(&mut *idle)
        };
        drop(evicted);
        None
    }

    /// recycle retains a completed buffer when this lease owns its only Arc.
    fn recycle(&self, buf: Arc<PinnedBuf>) {
        if self.closed.load(Ordering::Acquire) || Arc::strong_count(&buf) != 1 {
            return;
        }
        let mut idle = self.idle.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        idle.push(buf);
        drop(idle);
        self.changed.notify_one();
    }

    /// close stops future retention and releases every idle registration.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
        self.idle.lock().unwrap().clear();
    }

    /// stats returns a consistent-enough diagnostic snapshot.
    fn stats(&self) -> BufferPoolStats {
        let idle = self.idle.lock().unwrap();
        BufferPoolStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            cached_buffers: idle.len(),
            cached_bytes: idle.iter().map(|buf| buf.len()).sum(),
        }
    }
}

/// PooledBuf is an exclusive lease over a registered buffer. Dropping the lease returns the
/// buffer to its pool only after every operation-owned Arc has been reaped.
pub struct PooledBuf {
    /// buf is taken by Drop and recycled when it has no other owners.
    buf: Option<Arc<PinnedBuf>>,

    /// pool receives the completed buffer.
    pool: Arc<BufferPool>,

    /// logical_len is the transfer-visible prefix of the physical buffer.
    logical_len: usize,
}

impl PooledBuf {
    /// buffer returns the registered allocation for fabric post calls.
    pub(crate) fn buffer(&self) -> &Arc<PinnedBuf> {
        self.buf.as_ref().expect("pooled buffer")
    }

    /// len returns the transfer-visible length.
    pub fn len(&self) -> usize {
        self.logical_len
    }

    /// is_empty returns whether the transfer-visible range is empty.
    pub fn is_empty(&self) -> bool {
        self.logical_len == 0
    }

    /// as_mut_slice exposes only the transfer-visible prefix.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no operation over this buffer is currently posted.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer().as_mut_slice()[..self.logical_len]
    }

    /// as_slice exposes only the transfer-visible prefix for reading, and may be called
    /// concurrently for the same lease.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no operation over this buffer is currently posted.
    pub unsafe fn as_slice(&self) -> &[u8] {
        &self.buffer().as_slice()[..self.logical_len]
    }

    /// as_mut_range exposes one transfer-visible subrange for pipelined filling.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no fabric operation or other CPU reference currently accesses
    /// any byte in `range`.
    pub(crate) unsafe fn as_mut_range(&mut self, range: std::ops::Range<usize>) -> &mut [u8] {
        assert!(range.start <= range.end && range.end <= self.logical_len);
        self.buffer().as_mut_range(range)
    }

    /// into_reader turns a completed receive lease into an async reader without moving or
    /// copying its registered allocation.
    pub fn into_reader(self) -> PooledBufReader {
        PooledBufReader {
            buffer: self,
            position: 0,
        }
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.recycle(buf);
        }
    }
}

/// PooledBufReader reads a completed receive directly from registered memory. Its lease returns
/// the registration to the pool when the reader is consumed or dropped.
pub struct PooledBufReader {
    /// buffer owns the registered-memory lease.
    buffer: PooledBuf,

    /// position is the next byte exposed to the downstream storage writer.
    position: usize,
}

impl fmt::Debug for PooledBufReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PooledBufReader")
            .field("length", &self.buffer.len())
            .field("position", &self.position)
            .finish()
    }
}

impl AsyncRead for PooledBufReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.buffer.len().saturating_sub(self.position);
        if remaining == 0 || output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let read_len = remaining.min(output.remaining());
        let start = self.position;
        let end = start + read_len;
        // Safety: PooledBufReader is constructed only after every receive completion was
        // reaped, and it exclusively owns the lease while exposing this range.
        let content = unsafe { &self.buffer.as_slice()[start..end] };
        output.put_slice(content);
        self.position = end;
        Poll::Ready(Ok(()))
    }
}

/// OpHandle is a posted operation whose completion can be awaited exactly once.
pub struct OpHandle {
    /// ctx_addr identifies the operation in the pending map (and to fi_cancel).
    ctx_addr: usize,

    /// id pairs with ctx_addr so a handle dropped after its completion was reaped cannot cancel
    /// whichever operation has since been given the same context address.
    id: u64,

    /// rx receives the completion from the progress thread.
    rx: Option<oneshot::Receiver<Completion>>,

    /// inner lets Drop cancel an operation when its owner is abandoned by an early return or
    /// asynchronous task cancellation.
    inner: Arc<FabricInner>,

    /// armed remains true until wait observes a completion or explicitly finishes cancellation.
    armed: bool,
}

impl OpHandle {
    /// cancel requests cancellation while leaving the pending entry responsible for the context
    /// and buffer until the provider reports a completion.
    fn cancel(&self) {
        self.inner.cancel_ctx(self.ctx_addr, self.id);
    }
}

impl Drop for OpHandle {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

/// Fabric wraps one shared libfabric RDM endpoint. Share an instance across transfers for one
/// transport role; the downloader and server currently create separate instances. Endpoints are
/// heavyweight, especially on EFA.
pub struct Fabric {
    /// inner is shared with the progress thread and pinned buffers.
    inner: Arc<FabricInner>,

    /// progress is the completion-polling thread, joined on drop.
    progress: Option<std::thread::JoinHandle<()>>,

    /// provider is the concrete provider name selected at runtime (e.g. "efa",
    /// "verbs;ofi_rxm", "tcp").
    provider: String,

    /// local_endpoint is this endpoint's provider-opaque address (fi_getname) to advertise
    /// to peers.
    local_endpoint: Vec<u8>,

    /// max_msg_size is the provider's maximum single-message size.
    max_msg_size: usize,

    /// budget bounds registered/pinned memory, in BUDGET_UNIT permits.
    budget: Arc<Semaphore>,

    /// budget_permits is the total number of permits in the budget.
    budget_permits: u32,

    /// pool retains idle registrations for best-fit reuse.
    pool: Arc<BufferPool>,

    /// tag_counter is the first unallocated transfer-tag block.
    tag_counter: AtomicU64,
}

impl Fabric {
    /// new opens a fabric endpoint on the given provider ("efa", "verbs", "tcp", ...). When
    /// `provider` is None, hardware providers are tried in preference order; an unrestricted
    /// libfabric lookup is used only when `allow_software_provider` is explicitly enabled.
    /// `device` optionally pins a specific libfabric domain (e.g. "efa_0-rdm" or "rdmap16s27").
    pub fn new(
        provider: Option<&str>,
        device: Option<&str>,
        max_registered_bytes: u64,
        allow_software_provider: bool,
    ) -> Result<Self> {
        let device_cstr = device
            .map(CString::new)
            .transpose()
            .map_err(|_| Error::InvalidParameter)?;

        let mut handle: *mut ffi::DfrdmaFabric = std::ptr::null_mut();
        let candidates: Vec<Option<&str>> = match provider {
            Some(provider) => vec![Some(provider)],
            None if allow_software_provider => vec![Some("efa"), Some("verbs"), None],
            None => vec![Some("efa"), Some("verbs")],
        };
        let mut last_rc = 0;
        for candidate in candidates {
            let provider_cstr = candidate
                .map(CString::new)
                .transpose()
                .map_err(|_| Error::InvalidParameter)?;
            let mut candidate_handle: *mut ffi::DfrdmaFabric = std::ptr::null_mut();
            // Safety: the strings outlive the call; out pointer is valid.
            let rc = unsafe {
                ffi::dfrdma_open(
                    provider_cstr
                        .as_ref()
                        .map_or(std::ptr::null(), |s| s.as_ptr() as *const c_char),
                    device_cstr
                        .as_ref()
                        .map_or(std::ptr::null(), |s| s.as_ptr() as *const c_char),
                    &mut candidate_handle,
                )
            };
            if rc == 0 && !candidate_handle.is_null() {
                handle = candidate_handle;
                break;
            }
            last_rc = rc;
        }
        if handle.is_null() {
            return Err(fi_error("hardware provider discovery", last_rc as i64));
        }

        // Safety: handle is valid; provider name points into fi_info owned by the handle.
        let (provider_name, max_msg_size, mr_required) = unsafe {
            (
                CStr::from_ptr(ffi::dfrdma_provider_name(handle))
                    .to_string_lossy()
                    .into_owned(),
                ffi::dfrdma_max_msg_size(handle),
                ffi::dfrdma_mr_required(handle) != 0,
            )
        };

        let mut endpoint = vec![0u8; GETNAME_INITIAL_CAPACITY];
        let mut endpoint_len = endpoint.len();
        // Safety: buffer and length pointer are valid.
        let mut rc =
            unsafe { ffi::dfrdma_getname(handle, endpoint.as_mut_ptr(), &mut endpoint_len) };
        if rc == 1 {
            endpoint.resize(endpoint_len, 0);
            // Safety: buffer was resized to the length requested by the provider.
            rc = unsafe { ffi::dfrdma_getname(handle, endpoint.as_mut_ptr(), &mut endpoint_len) };
        }
        if rc != 0 {
            // Safety: handle is valid and not yet shared.
            unsafe { ffi::dfrdma_close(handle) };
            return Err(fi_error("fi_getname", rc as i64));
        }
        endpoint.truncate(endpoint_len);

        let budget_permits = (max_registered_bytes / BUDGET_UNIT)
            .max(1)
            .min(Semaphore::MAX_PERMITS as u64)
            .min(u32::MAX as u64) as u32;

        let handle = Arc::new(Handle {
            raw: handle,
            endpoint_lifecycle: RwLock::new(()),
            endpoint_open: AtomicBool::new(true),
            endpoint_close_drains: endpoint_close_drains(&provider_name),
        });
        let inner = Arc::new(FabricInner {
            cancel_progress_lock: Mutex::new(()),
            handle,
            pending: Mutex::new(HashMap::new()),
            op_counter: AtomicU64::new(0),
            av: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            failure_reason: Mutex::new(None),
            failure_notify: Notify::new(),
            mr_required,
        });

        let progress_inner = inner.clone();
        let progress = std::thread::Builder::new()
            .name("rdma-progress".to_string())
            .spawn(move || progress_loop(progress_inner))
            .map_err(|err| Error::Unknown(format!("failed to spawn rdma progress: {err}")))?;

        info!(
            "opened rdma fabric: provider {}, max message size {}, mr required {}, endpoint {} bytes",
            provider_name,
            max_msg_size,
            mr_required,
            endpoint.len()
        );

        Ok(Self {
            inner,
            progress: Some(progress),
            provider: provider_name,
            local_endpoint: endpoint,
            max_msg_size,
            budget: Arc::new(Semaphore::new(budget_permits as usize)),
            budget_permits,
            pool: Arc::new(BufferPool::new()),
            tag_counter: AtomicU64::new(0),
        })
    }

    /// provider returns the concrete provider name selected at runtime.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// is_failed reports whether the endpoint has been retired after an unrecoverable
    /// operation or completion-queue failure.
    pub fn is_failed(&self) -> bool {
        self.inner.failed.load(Ordering::Acquire)
    }

    /// local_endpoint returns the provider-opaque endpoint address to advertise to peers.
    pub fn local_endpoint(&self) -> &[u8] {
        &self.local_endpoint
    }

    /// max_msg_size returns the provider's maximum single-message size.
    pub fn max_msg_size(&self) -> usize {
        self.max_msg_size
    }

    /// buffer_pool_stats returns registered-buffer reuse and idle-memory counters.
    pub fn buffer_pool_stats(&self) -> BufferPoolStats {
        self.pool.stats()
    }

    /// registered_budget_bytes returns the ceiling on bytes held by active and pooled buffers
    /// together. Callers use it to decide how many buffers to hold at once, since
    /// [`Fabric::acquire_buffer`] blocks rather than reporting that the budget is spent.
    pub fn registered_budget_bytes(&self) -> u64 {
        u64::from(self.budget_permits) * BUDGET_UNIT
    }

    /// next_tag reserves a disjoint block of tags for one transfer. Exhaustion fails closed
    /// instead of wrapping into a block that may still belong to an in-flight transfer.
    pub fn next_tag(&self) -> Result<u64> {
        self.tag_counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(TAG_RANGE_SIZE)
            })
            .map_err(|_| Error::Unknown("rdma transfer tag space exhausted".to_string()))
    }

    /// alloc_buffer allocates a transfer buffer of `len` bytes, waits for registered-memory
    /// budget, and registers the buffer when the provider requires it. Production transfers
    /// should use [`Fabric::acquire_buffer`] so the registration is returned to the pool.
    pub async fn alloc_buffer(&self, len: usize) -> Result<Arc<PinnedBuf>> {
        let mut pooled = self.acquire_buffer(len).await?;
        Ok(pooled.buf.take().expect("pooled buffer"))
    }

    /// try_acquire_buffer checks out a best-fit registered buffer only if one is available right
    /// now, returning None instead of waiting.
    ///
    /// A caller that already holds a buffer must use this rather than [`Fabric::acquire_buffer`].
    /// Waiting for budget while holding a registration is how several transfers deadlock each
    /// other: each holds part of the budget and blocks for the rest. Returning None lets the
    /// caller proceed with what it has.
    pub fn try_acquire_buffer(&self, len: usize) -> Result<Option<PooledBuf>> {
        if len == 0 {
            return Err(Error::InvalidParameter);
        }
        let permits = self.buffer_permits(len)?;
        self.inner.ensure_healthy()?;
        if self.pool.closed.load(Ordering::Acquire) {
            return Err(Error::Unknown("rdma fabric is shut down".to_string()));
        }

        if let Some(buf) = self.pool.take_best_fit(len) {
            return Ok(Some(PooledBuf {
                buf: Some(buf),
                pool: self.pool.clone(),
                logical_len: len,
            }));
        }

        match self.budget.clone().try_acquire_many_owned(permits) {
            Ok(permit) => {
                let buf = self.register_buffer(len, permit)?;
                self.pool.misses.fetch_add(1, Ordering::Relaxed);
                Ok(Some(PooledBuf {
                    buf: Some(buf),
                    pool: self.pool.clone(),
                    logical_len: len,
                }))
            }
            Err(TryAcquireError::Closed) => {
                Err(Error::Unknown("rdma fabric is shut down".to_string()))
            }
            Err(TryAcquireError::NoPermits) => Ok(None),
        }
    }

    /// acquire_buffer checks out a best-fit registered buffer. It waits for either a returned
    /// registration or fresh budget, evicting undersized idle buffers to avoid permit starvation.
    pub async fn acquire_buffer(&self, len: usize) -> Result<PooledBuf> {
        if len == 0 {
            return Err(Error::InvalidParameter);
        }
        let permits = self.buffer_permits(len)?;

        loop {
            self.inner.ensure_healthy()?;
            if self.pool.closed.load(Ordering::Acquire) {
                return Err(Error::Unknown("rdma fabric is shut down".to_string()));
            }

            let changed = self.pool.changed.notified();
            let failed = self.inner.failure_notify.notified();
            if let Some(buf) = self.pool.take_best_fit(len) {
                return Ok(PooledBuf {
                    buf: Some(buf),
                    pool: self.pool.clone(),
                    logical_len: len,
                });
            }

            match self.budget.clone().try_acquire_many_owned(permits) {
                Ok(permit) => {
                    let buf = self.register_buffer(len, permit)?;
                    self.pool.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(PooledBuf {
                        buf: Some(buf),
                        pool: self.pool.clone(),
                        logical_len: len,
                    });
                }
                Err(TryAcquireError::Closed) => {
                    return Err(Error::Unknown("rdma fabric is shut down".to_string()));
                }
                Err(TryAcquireError::NoPermits) => {}
            }

            let budget = self.budget.clone();
            tokio::select! {
                _ = changed => continue,
                _ = failed => {
                    return Err(Error::Unknown(
                        self.inner
                            .failure()
                            .unwrap_or_else(|| "rdma fabric is unavailable".to_string()),
                    ));
                },
                permit = budget.acquire_many_owned(permits) => {
                    let permit = permit.map_err(|_| {
                        Error::Unknown("rdma fabric is shut down".to_string())
                    })?;
                    // A suitable registration may have arrived in parallel with the
                    // semaphore grant. Prefer it and return the redundant permits.
                    if let Some(buf) = self.pool.take_best_fit(len) {
                        drop(permit);
                        return Ok(PooledBuf {
                            buf: Some(buf),
                            pool: self.pool.clone(),
                            logical_len: len,
                        });
                    }
                    let buf = self.register_buffer(len, permit)?;
                    self.pool.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(PooledBuf {
                        buf: Some(buf),
                        pool: self.pool.clone(),
                        logical_len: len,
                    });
                }
            }
        }
    }

    /// buffer_permits validates a requested buffer and returns its budget units.
    fn buffer_permits(&self, len: usize) -> Result<u32> {
        let permits = len.div_ceil(BUDGET_UNIT as usize).max(1);
        if permits > self.budget_permits as usize {
            return Err(Error::Unknown(format!(
                "buffer of {len} bytes exceeds the rdma registered-memory budget"
            )));
        }
        Ok(permits as u32)
    }

    /// register_buffer allocates stable storage and registers it using an already-owned budget.
    fn register_buffer(&self, len: usize, permit: OwnedSemaphorePermit) -> Result<Arc<PinnedBuf>> {
        self.inner.ensure_healthy()?;
        let mut data = vec![0u8; len];
        let mut mr: *mut c_void = std::ptr::null_mut();
        let mut desc: *mut c_void = std::ptr::null_mut();
        // Safety: data outlives the registration; PinnedBuf's field order guarantees the
        // MrGuard closes the registration before the Vec is freed. FI_THREAD_SAFE permits
        // registration concurrently with endpoint and CQ operations.
        let rc = unsafe {
            ffi::dfrdma_mr_reg(
                self.inner.handle.raw,
                data.as_mut_ptr() as *mut c_void,
                len,
                &mut mr,
                &mut desc,
            )
        };
        if rc != 0 {
            if self.inner.mr_required {
                return Err(fi_error("fi_mr_reg", rc as i64));
            }
            warn!(
                "rdma memory registration failed ({}), continuing unregistered",
                rc
            );
            mr = std::ptr::null_mut();
            desc = std::ptr::null_mut();
        }

        Ok(Arc::new(PinnedBuf {
            mr_guard: MrGuard {
                mr,
                _handle: self.inner.handle.clone(),
            },
            desc,
            data: UnsafeCell::new(data),
            _permit: permit,
        }))
    }

    /// resolve inserts a peer endpoint address into the address vector, returning its
    /// fabric address. Results are cached.
    pub fn resolve(&self, endpoint: &[u8]) -> Result<u64> {
        self.inner.ensure_healthy()?;
        if endpoint.is_empty() {
            return Err(Error::InvalidParameter);
        }
        let _lifecycle = self.inner.handle.endpoint_lifecycle.read().unwrap();
        if !self.inner.handle.endpoint_open.load(Ordering::Acquire) {
            return Err(Error::Unknown("rdma endpoint is closed".to_string()));
        }
        // Hold the cache lock through insertion to avoid racing duplicate AV entries.
        let mut av = self.inner.av.lock().unwrap();
        if let Some(addr) = av.get(endpoint) {
            return Ok(*addr);
        }
        if av.len() >= MAX_RESOLVED_PEERS {
            return Err(Error::Unknown(
                "rdma resolved-peer address cache is full".to_string(),
            ));
        }

        let mut addr: u64 = 0;
        // Safety: endpoint bytes are valid for the call and FI_THREAD_SAFE permits
        // concurrent access to the address vector and endpoint.
        let rc = unsafe {
            ffi::dfrdma_av_insert(
                self.inner.handle.raw,
                endpoint.as_ptr(),
                endpoint.len(),
                &mut addr,
            )
        };
        if rc != 0 {
            return Err(fi_error("fi_av_insert", rc as i64));
        }

        av.insert(endpoint.to_vec(), addr);
        Ok(addr)
    }

    /// post_recv posts a tagged receive of `len` bytes into `buf` at `offset`.
    pub async fn post_recv(
        &self,
        buf: &Arc<PinnedBuf>,
        offset: usize,
        len: usize,
        tag: u64,
    ) -> Result<OpHandle> {
        self.post(buf, offset, len, tag, None).await
    }

    /// post_send posts a tagged send of `len` bytes from `buf` at `offset` to `dest`.
    pub async fn post_send(
        &self,
        buf: &Arc<PinnedBuf>,
        offset: usize,
        len: usize,
        tag: u64,
        dest: u64,
    ) -> Result<OpHandle> {
        self.post(buf, offset, len, tag, Some(dest)).await
    }

    /// post registers the pending operation, then posts it, retrying while the queue is
    /// full. `dest` selects send (Some) or receive (None).
    async fn post(
        &self,
        buf: &Arc<PinnedBuf>,
        offset: usize,
        len: usize,
        tag: u64,
        dest: Option<u64>,
    ) -> Result<OpHandle> {
        self.inner.ensure_healthy()?;
        if offset.checked_add(len).is_none_or(|end| end > buf.len()) {
            return Err(Error::InvalidParameter);
        }

        let ctx = Box::new(CtxBlock([0u8; 128]));
        let ctx_addr = &*ctx as *const CtxBlock as usize;
        let id = self.inner.op_counter.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        // Register the pending operation before posting so a completion arriving
        // immediately after the post always finds it.
        self.inner.pending.lock().unwrap().insert(
            ctx_addr,
            PendingOp {
                id,
                tx,
                _ctx: ctx,
                _buf: buf.clone(),
            },
        );

        let deadline = tokio::time::Instant::now() + POST_RETRY_TIMEOUT;
        loop {
            if let Err(err) = self.inner.ensure_healthy() {
                self.inner.pending.lock().unwrap().remove(&ctx_addr);
                return Err(err);
            }
            let rc = {
                let _lifecycle = self.inner.handle.endpoint_lifecycle.read().unwrap();
                if !self.inner.handle.endpoint_open.load(Ordering::Acquire) {
                    self.inner.pending.lock().unwrap().remove(&ctx_addr);
                    return Err(Error::Unknown("rdma endpoint is closed".to_string()));
                }
                // Safety: buf outlives the operation via the pending map; the range was
                // validated above; ctx_addr points at the boxed context block owned by the
                // pending map. The shim requires FI_THREAD_SAFE, so posts may run concurrently.
                unsafe {
                    match dest {
                        Some(dest) => ffi::dfrdma_tsend(
                            self.inner.handle.raw,
                            buf.ptr(offset) as *const c_void,
                            len,
                            buf.desc,
                            dest,
                            tag,
                            ctx_addr as *mut c_void,
                        ),
                        None => ffi::dfrdma_trecv(
                            self.inner.handle.raw,
                            buf.ptr(offset) as *mut c_void,
                            len,
                            buf.desc,
                            tag,
                            ctx_addr as *mut c_void,
                        ),
                    }
                }
            };

            match rc {
                0 => {
                    return Ok(OpHandle {
                        ctx_addr,
                        id,
                        rx: Some(rx),
                        inner: self.inner.clone(),
                        armed: true,
                    })
                }
                1 => {
                    if tokio::time::Instant::now() >= deadline {
                        self.inner.pending.lock().unwrap().remove(&ctx_addr);
                        return Err(Error::Unknown(
                            "rdma post retries exhausted, queue stayed full".to_string(),
                        ));
                    }
                    tokio::time::sleep(POST_RETRY_INTERVAL).await;
                }
                rc => {
                    self.inner.pending.lock().unwrap().remove(&ctx_addr);
                    let op = if dest.is_some() {
                        "fi_tsend"
                    } else {
                        "fi_trecv"
                    };
                    return Err(fi_error(op, rc));
                }
            }
        }
    }

    /// wait awaits an operation's completion, returning the transferred length. On timeout
    /// the operation is cancelled; if the cancellation completion does not arrive within a
    /// grace period, the buffer is left pinned (leaked) rather than freed under the NIC.
    pub async fn wait(&self, mut op: OpHandle, timeout: Duration) -> Result<usize> {
        let ctx_addr = op.ctx_addr;
        let op_id = op.id;
        let rx = op.rx.take().expect("rdma operation receiver");
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(completion)) if completion.err == 0 => {
                op.armed = false;
                Ok(completion.len)
            }
            Ok(Ok(completion)) => {
                op.armed = false;
                Err(fi_error("operation", completion.err))
            }
            Ok(Err(_)) => {
                op.armed = false;
                Err(Error::Unknown("rdma fabric is shut down".to_string()))
            }
            Err(_) => {
                op.cancel();

                // Wait for the cancellation (or late) completion so the pending map entry
                // and its buffer reference are released.
                let deadline = tokio::time::Instant::now() + CANCEL_GRACE_TIMEOUT;
                loop {
                    if !self.inner.is_pending(ctx_addr, op_id) {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        error!("rdma operation neither completed nor cancelled; retiring endpoint");
                        self.inner.fail_and_abort(
                            "rdma operation cancellation grace period expired".to_string(),
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                op.armed = false;
                Err(Error::Unknown("rdma operation timed out".to_string()))
            }
        }
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        // Close the endpoint before releasing pending buffers. This is required for EFA posted
        // receives whose peer disappeared and also prevents FabricInner's defensive drop path
        // from having to retain a live DMA buffer forever during ordinary shutdown.
        self.inner
            .fail_and_abort("rdma fabric shutting down".to_string());
        self.inner.shutdown.store(true, Ordering::Relaxed);
        if let Some(progress) = self.progress.take() {
            let _ = progress.join();
        }
        // Release idle registrations before the endpoint handle can close. Leases still held by
        // downstream readers observe the closed pool and release their buffers on drop.
        self.pool.close();
    }
}

/// progress_loop polls the completion queue and routes completions to waiting tasks. It is
/// the only place pending-map entries (and thus buffer references) are released on the
/// success path.
fn progress_loop(inner: Arc<FabricInner>) {
    let mut active_yields = 0u32;
    while !inner.shutdown.load(Ordering::Relaxed) && !inner.failed.load(Ordering::Acquire) {
        let mut progressed = false;
        loop {
            let mut entries = [ffi::DfrdmaCompletion {
                context: std::ptr::null_mut(),
                flags: 0,
                len: 0,
                err: 0,
            }; CQ_BATCH_SIZE];
            let mut completed: [Option<(Completion, PendingOp)>; CQ_BATCH_SIZE] =
                std::array::from_fn(|_| None);
            let rc = {
                // Keep cancellation from observing these contexts until every CQ result is
                // removed from pending. Posts and registrations remain concurrent.
                let _guard = inner.cancel_progress_lock.lock().unwrap();
                // Safety: the handle is valid until FabricInner drops, which cannot happen
                // while this thread holds an Arc to it. FI_THREAD_SAFE permits concurrent
                // posts on other threads; entries has the capacity passed to the shim.
                let _lifecycle = inner.handle.endpoint_lifecycle.read().unwrap();
                if !inner.handle.endpoint_open.load(Ordering::Acquire) {
                    return;
                }
                let rc = unsafe {
                    ffi::dfrdma_cq_read_batch(inner.handle.raw, entries.as_mut_ptr(), entries.len())
                };
                if rc > 0 {
                    // Batched removal under the cancellation/CQ lock closes the lifetime gap
                    // between cancel_ctx's pending lookup and its fi_cancel call.
                    let mut pending = inner.pending.lock().unwrap();
                    for index in 0..rc as usize {
                        if let Some(op) = pending.remove(&(entries[index].context as usize)) {
                            completed[index] = Some((
                                Completion {
                                    len: entries[index].len,
                                    err: entries[index].err,
                                },
                                op,
                            ));
                        }
                    }
                }
                rc
            };

            match rc {
                count if count > 0 => {
                    progressed = true;
                    // Deliver and drop outside both locks because dropping an operation may
                    // close its memory region.
                    for (index, completion) in
                        completed.into_iter().take(count as usize).enumerate()
                    {
                        if let Some((completion, op)) = completion {
                            // The receiver may have timed out and gone; that is fine, the
                            // buffer reference is released either way.
                            let _ = op.tx.send(completion);
                        } else {
                            let _ = entries[index].flags;
                            warn!("rdma completion for unknown context, dropping");
                        }
                    }
                }
                0 => break,
                rc => {
                    inner.fail_and_abort(format!("rdma completion queue read failed: {rc}"));
                    return;
                }
            }
        }

        if progressed {
            active_yields = 0;
        } else if inner.pending.lock().unwrap().is_empty() {
            active_yields = 0;
            std::thread::sleep(PROGRESS_IDLE_INTERVAL);
        } else if active_yields < PROGRESS_ACTIVE_YIELDS {
            active_yields += 1;
            std::thread::yield_now();
        } else {
            active_yields = 0;
            std::thread::sleep(PROGRESS_ACTIVE_INTERVAL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// open_fabric opens a fabric on whatever provider is available (on development hosts
    /// without RDMA hardware libfabric selects its tcp or sockets provider, exercising the
    /// exact same code path as efa/verbs).
    fn open_fabric() -> Fabric {
        Fabric::new(None, None, 64 * 1024 * 1024, true).expect("libfabric endpoint")
    }

    #[test]
    fn allocates_disjoint_transfer_tag_ranges_and_fails_on_exhaustion() {
        let fabric = open_fabric();
        let first = fabric.next_tag().unwrap();
        let second = fabric.next_tag().unwrap();
        assert_eq!(second - first, TAG_RANGE_SIZE);
        assert!(first + TAG_RANGE_SIZE - 1 < second);

        fabric
            .tag_counter
            .store(u64::MAX - TAG_RANGE_SIZE + 1, Ordering::Relaxed);
        let err = fabric.next_tag().unwrap_err();
        assert!(err.to_string().contains("tag space exhausted"));
    }

    #[test]
    fn automatic_provider_never_silently_selects_software() {
        if let Ok(fabric) = Fabric::new(None, None, 64 * 1024 * 1024, false) {
            assert!(
                fabric.provider() == "efa" || fabric.provider().starts_with("verbs"),
                "unexpected automatic provider: {}",
                fabric.provider()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transfers_chunked_messages_between_endpoints() {
        let sender = open_fabric();
        let receiver = open_fabric();
        assert_eq!(sender.provider(), receiver.provider());
        assert!(!receiver.local_endpoint().is_empty());

        // 10 MiB payload in 4 MiB chunks: two full chunks and one partial.
        let length: usize = 10 * 1024 * 1024;
        let chunk_size: usize = (4 * 1024 * 1024).min(sender.max_msg_size());
        let payload: Vec<u8> = (0..length).map(|i| (i % 251) as u8).collect();
        let tag = sender.next_tag().unwrap();

        let send_buf = sender.alloc_buffer(length).await.unwrap();
        // Safety: nothing is posted over the buffer yet.
        unsafe { send_buf.as_mut_slice() }.copy_from_slice(&payload);

        // Receiver posts every chunk before the sender transmits (rendezvous ordering).
        let recv_buf = receiver.alloc_buffer(length).await.unwrap();
        let mut recv_ops = Vec::new();
        let mut offset = 0;
        let mut chunk = 0u64;
        while offset < length {
            let len = chunk_size.min(length - offset);
            recv_ops.push((
                len,
                receiver
                    .post_recv(&recv_buf, offset, len, tag + chunk)
                    .await
                    .unwrap(),
            ));
            offset += len;
            chunk += 1;
        }

        let dest = sender.resolve(receiver.local_endpoint()).unwrap();
        let mut send_ops = Vec::new();
        let mut offset = 0;
        let mut chunk = 0u64;
        while offset < length {
            let len = chunk_size.min(length - offset);
            send_ops.push(
                sender
                    .post_send(&send_buf, offset, len, tag + chunk, dest)
                    .await
                    .unwrap(),
            );
            offset += len;
            chunk += 1;
        }

        let timeout = Duration::from_secs(10);
        for op in send_ops {
            sender.wait(op, timeout).await.unwrap();
        }
        for (expected_len, op) in recv_ops {
            let len = receiver.wait(op, timeout).await.unwrap();
            assert_eq!(len, expected_len);
        }

        assert_eq!(recv_buf.into_vec(), payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_an_operation_cancels_and_reaps_it() {
        let fabric = open_fabric();
        let buf = fabric.alloc_buffer(4096).await.unwrap();
        let op = fabric
            .post_recv(&buf, 0, 4096, fabric.next_tag().unwrap())
            .await
            .unwrap();
        assert_eq!(fabric.inner.pending.lock().unwrap().len(), 1);

        drop(op);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !fabric.inner.pending.lock().unwrap().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancelled operation was not reaped"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_buffers_over_the_registered_memory_budget() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        assert!(fabric.alloc_buffer(2 * 1024 * 1024).await.is_err());
        assert!(fabric.alloc_buffer(512 * 1024).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_buffers_reuse_the_best_fit_registration() {
        let fabric = Fabric::new(None, None, 4 * 1024 * 1024, true).expect("libfabric endpoint");
        let first = fabric.acquire_buffer(1024 * 1024).await.unwrap();
        let first_ptr = Arc::as_ptr(first.buffer());
        drop(first);

        let second = fabric.acquire_buffer(512 * 1024).await.unwrap();
        assert_eq!(Arc::as_ptr(second.buffer()), first_ptr);
        assert_eq!(second.len(), 512 * 1024);
        assert_eq!(second.buffer().len(), 1024 * 1024);

        let stats = fabric.buffer_pool_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_reader_hides_unused_capacity_and_recycles() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        let mut buffer = fabric.acquire_buffer(4096).await.unwrap();
        // Safety: this lease has not been posted.
        unsafe { buffer.as_mut_slice() }.fill(0x5a);
        drop(buffer);

        let smaller = fabric.acquire_buffer(17).await.unwrap();
        let mut reader = smaller.into_reader();
        let mut content = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut content)
            .await
            .unwrap();
        assert_eq!(content, vec![0x5a; 17]);
        drop(reader);

        let stats = fabric.buffer_pool_stats();
        assert_eq!(stats.cached_buffers, 1);
        assert_eq!(stats.cached_bytes, 4096);
    }

    #[test]
    fn only_known_providers_treat_endpoint_close_as_a_dma_barrier() {
        for provider in ["efa", "verbs", "verbs;ofi_rxm", "tcp", "shm"] {
            assert!(endpoint_close_drains(provider), "{provider}");
        }
        for provider in ["cxi", "opx", "psm3", ""] {
            assert!(!endpoint_close_drains(provider), "{provider}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_acquire_buffer_reports_an_exhausted_budget_instead_of_waiting() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        assert_eq!(fabric.registered_budget_bytes(), 1024 * 1024);

        let held = fabric.try_acquire_buffer(1024 * 1024).unwrap();
        assert!(held.is_some());

        // The budget is spent, so a caller already holding a registration is told so rather than
        // being parked while it holds memory another transfer is waiting for.
        assert!(fabric.try_acquire_buffer(1024 * 1024).unwrap().is_none());

        drop(held);
        assert!(fabric.try_acquire_buffer(1024 * 1024).unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_waiter_reuses_returned_full_budget_buffer() {
        let fabric =
            Arc::new(Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint"));
        let first = fabric.acquire_buffer(1024 * 1024).await.unwrap();
        let first_ptr = Arc::as_ptr(first.buffer()) as usize;

        let waiting_fabric = fabric.clone();
        let waiter = tokio::spawn(async move { waiting_fabric.acquire_buffer(1024 * 1024).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("pool waiter timed out")
            .unwrap()
            .unwrap();
        assert_eq!(Arc::as_ptr(second.buffer()) as usize, first_ptr);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retired_fabric_wakes_buffer_waiters() {
        let fabric =
            Arc::new(Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint"));
        let held = fabric.acquire_buffer(1024 * 1024).await.unwrap();

        let waiting_fabric = fabric.clone();
        let waiter = tokio::spawn(async move { waiting_fabric.acquire_buffer(1024 * 1024).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        fabric
            .inner
            .fail_and_abort("test endpoint failure".to_string());
        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("buffer waiter did not wake")
            .expect("buffer waiter task panicked");
        assert!(result.is_err());
        assert!(fabric.is_failed());
        drop(held);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn in_flight_buffer_is_not_returned_to_pool() {
        let fabric = open_fabric();
        let buffer = fabric.acquire_buffer(4096).await.unwrap();
        let op = fabric
            .post_recv(buffer.buffer(), 0, 4096, fabric.next_tag().unwrap())
            .await
            .unwrap();
        drop(buffer);
        assert_eq!(fabric.buffer_pool_stats().cached_buffers, 0);

        drop(op);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !fabric.inner.pending.lock().unwrap().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancelled operation was not reaped"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fabric.buffer_pool_stats().cached_buffers, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn closing_pool_releases_idle_registered_budget() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        let buffer = fabric.acquire_buffer(1024 * 1024).await.unwrap();
        drop(buffer);
        assert_eq!(fabric.budget.available_permits(), 0);
        assert_eq!(fabric.buffer_pool_stats().cached_bytes, 1024 * 1024);

        fabric.pool.close();
        assert_eq!(
            fabric.budget.available_permits(),
            fabric.budget_permits as usize
        );
        assert_eq!(fabric.buffer_pool_stats().cached_buffers, 0);
    }
}
