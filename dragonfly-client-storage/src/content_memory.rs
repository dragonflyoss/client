use bytes::{Bytes, BytesMut};
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::buffer_pool::BufferPool;
use futures::{Stream, TryStreamExt};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::Instant;

struct Task {
    content_length: u64,
    pieces: HashMap<u64, Bytes>,
    last_accessed: Instant,
    last_used: u64,
}

#[derive(Default)]
struct State {
    tasks: HashMap<String, Task>,
    size: u64,
    clock: u64,
}

pub struct MemoryContent {
    capacity: u64,
    ttl: Duration,
    read_buffer_size: usize,
    buffer_pool: BufferPool,
    state: Arc<RwLock<State>>,
}

impl MemoryContent {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            capacity: config.storage.capacity.as_u64(),
            ttl: config.gc.policy.task_ttl,
            read_buffer_size: config.storage.read_buffer_size,
            buffer_pool: BufferPool::new(0),
            state: Arc::new(RwLock::new(State::default())),
        }
    }

    pub fn total_space(&self) -> Result<u64> {
        Ok(self.capacity)
    }

    pub fn available_space(&self) -> Result<u64> {
        let state = self.state.read().unwrap();
        Ok(self.capacity.saturating_sub(state.size))
    }

    pub fn has_enough_space(&self, content_length: u64) -> Result<bool> {
        Ok(content_length <= self.capacity)
    }

    #[cfg(test)]
    pub fn used_space(&self) -> u64 {
        self.state.read().unwrap().size
    }

    pub async fn create_task(&self, task_id: &str, content_length: u64) -> Result<Vec<String>> {
        if content_length > self.capacity {
            return Err(Error::NoSpace(format!(
                "task {task_id} length {content_length} exceeds memory capacity {}",
                self.capacity
            )));
        }

        let mut state = self.state.write().unwrap();
        let now = Instant::now();
        let mut evicted = self.remove_expired(&mut state, now);
        if state.tasks.contains_key(task_id) {
            Self::touch_task(&mut state, task_id, now);
            return Ok(evicted);
        }

        while state.size.saturating_add(content_length) > self.capacity {
            let Some(oldest_id) = state
                .tasks
                .iter()
                .min_by(|(left_id, left), (right_id, right)| {
                    left.last_used
                        .cmp(&right.last_used)
                        .then_with(|| left_id.cmp(right_id))
                })
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            Self::remove_task(&mut state, &oldest_id);
            evicted.push(oldest_id);
        }

        let clock = Self::next_clock(&mut state);
        state.size = state.size.saturating_add(content_length);
        state.tasks.insert(
            task_id.to_string(),
            Task {
                content_length,
                pieces: HashMap::new(),
                last_accessed: now,
                last_used: clock,
            },
        );
        Ok(evicted)
    }

    pub fn contains_task(&self, task_id: &str) -> bool {
        let state = self.state.read().unwrap();
        let now = Instant::now();
        state
            .tasks
            .get(task_id)
            .is_some_and(|task| !self.is_expired(task, now))
    }

    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        let mut state = self.state.write().unwrap();
        Self::remove_task(&mut state, task_id)
            .map(|_| ())
            .ok_or_else(|| Error::TaskNotFound(task_id.to_string()))
    }

    pub async fn copy_task(&self, task_id: &str, to: &Path) -> Result<()> {
        let (content_length, pieces) = {
            let mut state = self.state.write().unwrap();
            let now = Instant::now();
            let task = state
                .tasks
                .get(task_id)
                .ok_or_else(|| Error::TaskNotFound(task_id.to_string()))?;
            if self.is_expired(task, now) {
                return Err(Error::TaskNotFound(task_id.to_string()));
            }

            let content_length = task.content_length;
            let pieces = task.pieces.clone();
            Self::touch_task(&mut state, task_id, now);
            (content_length, pieces)
        };

        let length = usize::try_from(content_length).map_err(|_| Error::InvalidParameter)?;
        let mut content = BytesMut::zeroed(length);
        for (offset, piece) in pieces {
            let start = usize::try_from(offset).map_err(|_| Error::InvalidParameter)?;
            let end = start
                .checked_add(piece.len())
                .ok_or(Error::InvalidParameter)?;
            if end > content.len() {
                return Err(Error::InvalidParameter);
            }
            content[start..end].copy_from_slice(&piece);
        }

        tokio::fs::write(to, content).await?;
        Ok(())
    }

    pub async fn hard_link_task(&self, _task_id: &str, _to: &Path) -> Result<()> {
        Err(Error::Unimplemented)
    }

    pub async fn fadvise_dontneed_task(&self, _task_id: &str) -> Result<()> {
        Ok(())
    }

    pub async fn is_same_dev_inode_as_task(&self, _task_id: &str, _to: &Path) -> Result<bool> {
        Ok(false)
    }

    pub async fn read_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<super::io::RangeReader> {
        let content = {
            let mut state = self.state.write().unwrap();
            let now = Instant::now();
            let task = state
                .tasks
                .get(task_id)
                .ok_or_else(|| Error::TaskNotFound(task_id.to_string()))?;
            if self.is_expired(task, now) {
                return Err(Error::TaskNotFound(task_id.to_string()));
            }

            let content = task
                .pieces
                .get(&offset)
                .cloned()
                .ok_or_else(|| Error::PieceNotFound(format!("{task_id} at offset {offset}")))?;
            let (start, target_length) = Self::calculate_range(offset, length, range)?;
            let end = start
                .checked_add(target_length)
                .ok_or(Error::InvalidParameter)?;
            if end > content.len() as u64 {
                return Err(Error::InvalidParameter);
            }

            Self::touch_task(&mut state, task_id, now);
            content.slice(start as usize..end as usize)
        };

        Ok(super::io::RangeReader::new_memory(
            content,
            self.read_buffer_size,
            self.buffer_pool.clone(),
        ))
    }

    pub async fn write_piece_from_stream<S>(
        &self,
        task_id: &str,
        offset: u64,
        expected_length: u64,
        _is_prefetch: bool,
        stream: &mut S,
    ) -> Result<super::io::WriteRangeResponse>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + ?Sized,
    {
        self.validate_write(task_id, offset, expected_length)?;
        let mut content = BytesMut::new();
        let mut hasher = crc32fast::Hasher::new();
        let mut length = 0;
        while length < expected_length {
            let Some(mut chunk) = stream.try_next().await? else {
                break;
            };
            if chunk.is_empty() {
                continue;
            }

            let remaining = expected_length - length;
            if chunk.len() as u64 > remaining {
                chunk.truncate(remaining as usize);
            }
            hasher.update(&chunk);
            length += chunk.len() as u64;
            content.extend_from_slice(&chunk);
        }

        if length != expected_length {
            return Err(Error::Unknown(format!(
                "expected length {expected_length} but got {length}"
            )));
        }

        let mut state = self.state.write().unwrap();
        let now = Instant::now();
        Self::validate_task_write(&state, task_id, offset, expected_length, self.ttl, now)?;
        let task = state
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| Error::TaskNotFound(task_id.to_string()))?;
        let end = offset + expected_length;
        if task.pieces.iter().any(|(piece_offset, piece)| {
            *piece_offset != offset
                && *piece_offset < end
                && offset < piece_offset.saturating_add(piece.len() as u64)
        }) {
            return Err(Error::InvalidParameter);
        }
        task.pieces.insert(offset, content.freeze());
        Self::touch_task(&mut state, task_id, now);
        Ok(super::io::WriteRangeResponse {
            length,
            hash: hasher.finalize().to_string(),
        })
    }

    pub fn gc(&self) -> Vec<String> {
        let mut state = self.state.write().unwrap();
        self.remove_expired(&mut state, Instant::now())
    }

    fn validate_write(&self, task_id: &str, offset: u64, length: u64) -> Result<()> {
        let state = self.state.read().unwrap();
        Self::validate_task_write(&state, task_id, offset, length, self.ttl, Instant::now())
    }

    fn validate_task_write(
        state: &State,
        task_id: &str,
        offset: u64,
        length: u64,
        ttl: Duration,
        now: Instant,
    ) -> Result<()> {
        let task = state
            .tasks
            .get(task_id)
            .ok_or_else(|| Error::TaskNotFound(task_id.to_string()))?;
        if now.duration_since(task.last_accessed) >= ttl {
            return Err(Error::TaskNotFound(task_id.to_string()));
        }

        let end = offset.checked_add(length).ok_or(Error::InvalidParameter)?;
        if end > task.content_length {
            return Err(Error::InvalidParameter);
        }
        Ok(())
    }

    fn calculate_range(offset: u64, length: u64, range: Option<Range>) -> Result<(u64, u64)> {
        let Some(range) = range else {
            return Ok((0, length));
        };
        if range.length == 0 {
            return Err(Error::InvalidParameter);
        }

        let piece_end = offset.checked_add(length).ok_or(Error::InvalidParameter)?;
        let range_end = range
            .start
            .checked_add(range.length)
            .ok_or(Error::InvalidParameter)?;
        let start = offset.max(range.start);
        let end = piece_end.min(range_end);
        if start >= end {
            return Err(Error::InvalidParameter);
        }
        Ok((start - offset, end - start))
    }

    fn is_expired(&self, task: &Task, now: Instant) -> bool {
        now.duration_since(task.last_accessed) >= self.ttl
    }

    fn remove_expired(&self, state: &mut State, now: Instant) -> Vec<String> {
        let expired = state
            .tasks
            .iter()
            .filter(|(_, task)| self.is_expired(task, now))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &expired {
            Self::remove_task(state, id);
        }
        expired
    }

    fn remove_task(state: &mut State, task_id: &str) -> Option<Task> {
        let task = state.tasks.remove(task_id)?;
        state.size = state.size.saturating_sub(task.content_length);
        Some(task)
    }

    fn touch_task(state: &mut State, task_id: &str, now: Instant) {
        let clock = Self::next_clock(state);
        if let Some(task) = state.tasks.get_mut(task_id) {
            task.last_accessed = now;
            task.last_used = clock;
        }
    }

    fn next_clock(state: &mut State) -> u64 {
        state.clock = state.clock.wrapping_add(1);
        state.clock
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytesize::ByteSize;
    use dragonfly_client_config::dfdaemon::StorageType;
    use futures::stream;
    use std::path::PathBuf;
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    const TASK_ID: &str = "d3add1f66b0d0b8083f14479d6e181ec9e2b34cf07d4a1a2ee2fcf51d3a3f14a";

    fn config(capacity: u64, ttl: Duration) -> Arc<Config> {
        let mut config = Config::default();
        config.storage.kind = StorageType::Memory;
        config.storage.capacity = ByteSize::b(capacity);
        config.storage.read_buffer_size = 3;
        config.gc.policy.task_ttl = ttl;
        Arc::new(config)
    }

    async fn write(content: &MemoryContent, task_id: &str, offset: u64, bytes: &'static [u8]) {
        let mut stream = stream::iter([Ok(Bytes::from_static(bytes))]);
        content
            .write_piece_from_stream(task_id, offset, bytes.len() as u64, false, &mut stream)
            .await
            .unwrap();
    }

    async fn read(
        content: &MemoryContent,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let mut reader = content.read_piece(task_id, offset, length, None).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

    #[tokio::test]
    async fn read_refreshes_lru_and_capacity_evicts_the_coldest_task() {
        let content = MemoryContent::new(config(4, Duration::from_secs(60)));
        content.create_task("task1", 2).await.unwrap();
        write(&content, "task1", 0, b"aa").await;
        content.create_task("task2", 2).await.unwrap();
        write(&content, "task2", 0, b"bb").await;
        assert_eq!(read(&content, "task1", 0, 2).await.unwrap(), b"aa");

        let evicted = content.create_task("task3", 2).await.unwrap();
        assert_eq!(evicted, vec!["task2"]);
        assert!(content.contains_task("task1"));
        assert!(!content.contains_task("task2"));
        assert!(content.contains_task("task3"));
        assert_eq!(content.used_space(), 4);
        assert_eq!(content.available_space().unwrap(), 0);
        assert!(matches!(
            read(&content, "task2", 0, 2).await,
            Err(Error::TaskNotFound(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_tasks_are_unreadable_and_gc_releases_capacity() {
        let content = MemoryContent::new(config(4, Duration::from_secs(5)));
        content.create_task("task1", 4).await.unwrap();
        write(&content, "task1", 0, b"data").await;
        tokio::time::advance(Duration::from_secs(6)).await;

        assert!(matches!(
            read(&content, "task1", 0, 4).await,
            Err(Error::TaskNotFound(_))
        ));
        assert_eq!(content.used_space(), 4);
        assert_eq!(content.gc(), vec!["task1"]);
        assert_eq!(content.used_space(), 0);
        assert_eq!(content.available_space().unwrap(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_piece_reads_and_writes_stay_within_capacity() {
        let content = Arc::new(MemoryContent::new(config(64, Duration::from_secs(60))));
        content.create_task("task1", 64).await.unwrap();
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..32u8 {
            let content = content.clone();
            join_set.spawn(async move {
                let bytes = Bytes::from(vec![i]);
                let mut stream = stream::iter([Ok(bytes)]);
                content
                    .write_piece_from_stream("task1", i as u64, 1, false, &mut stream)
                    .await
                    .unwrap();
                let data = read(&content, "task1", i as u64, 1).await.unwrap();
                assert_eq!(data, vec![i]);
                assert!(content.used_space() <= 64);
            });
        }
        while let Some(result) = join_set.join_next().await {
            result.unwrap();
        }
        assert_eq!(content.used_space(), 64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_task_creation_never_exceeds_capacity() {
        let content = Arc::new(MemoryContent::new(config(64, Duration::from_secs(60))));
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..64 {
            let content = content.clone();
            join_set.spawn(async move {
                content.create_task(&format!("task{i}"), 8).await.unwrap();
                assert!(content.used_space() <= 64);
            });
        }
        while let Some(result) = join_set.join_next().await {
            result.unwrap();
        }
        assert!(content.used_space() <= 64);
    }

    #[tokio::test]
    async fn invalid_writes_and_ranges_do_not_access_out_of_bounds() {
        let content = MemoryContent::new(config(4, Duration::from_secs(60)));
        content.create_task("task1", 4).await.unwrap();
        let mut stream = stream::iter([Ok(Bytes::from_static(b"data"))]);
        assert!(matches!(
            content
                .write_piece_from_stream("task1", 1, 4, false, &mut stream)
                .await,
            Err(Error::InvalidParameter)
        ));
        assert!(content.create_task("oversized", 5).await.is_err());
        write(&content, "task1", 0, b"data").await;
        let mut stream = stream::iter([Ok(Bytes::from_static(b"at"))]);
        assert!(matches!(
            content
                .write_piece_from_stream("task1", 1, 2, false, &mut stream)
                .await,
            Err(Error::InvalidParameter)
        ));
        assert!(matches!(
            content
                .read_piece(
                    "task1",
                    0,
                    4,
                    Some(Range {
                        start: 4,
                        length: 1,
                    }),
                )
                .await,
            Err(Error::InvalidParameter)
        ));
    }

    #[tokio::test]
    async fn disk_and_memory_backends_pass_the_same_content_operations() {
        macro_rules! exercise_backend {
            ($backend:expr, $output:expr) => {{
                $backend.create_task(TASK_ID, 13).await.unwrap();
                let mut stream = stream::iter([
                    Ok(Bytes::from_static(b"piece ")),
                    Ok(Bytes::from_static(b"content")),
                ]);
                let response = $backend
                    .write_piece_from_stream(TASK_ID, 0, 13, false, &mut stream)
                    .await
                    .unwrap();
                assert_eq!(response.length, 13);
                let mut reader = $backend
                    .read_piece(
                        TASK_ID,
                        0,
                        13,
                        Some(Range {
                            start: 2,
                            length: 4,
                        }),
                    )
                    .await
                    .unwrap();
                let mut ranged = Vec::new();
                reader.read_to_end(&mut ranged).await.unwrap();
                assert_eq!(ranged, b"ece ");
                $backend.copy_task(TASK_ID, $output).await.unwrap();
                assert_eq!(tokio::fs::read($output).await.unwrap(), b"piece content");
                $backend.delete_task(TASK_ID).await.unwrap();
            }};
        }

        let disk_dir = tempdir().unwrap();
        let disk = crate::content::Content::new(Arc::new(Config::default()), disk_dir.path())
            .await
            .unwrap();
        exercise_backend!(disk, &disk_dir.path().join("output"));

        let memory_dir = tempdir().unwrap();
        let memory = MemoryContent::new(config(13, Duration::from_secs(60)));
        exercise_backend!(memory, &PathBuf::from(memory_dir.path()).join("output"));
    }
}
