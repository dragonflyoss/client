/*
 *     Copyright 2025 The Dragonfly Authors
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

use dragonfly_client_core::{Error, Result};

/// The length of the file header holding the encryption parameters and the
/// wrapped data encryption key, in bytes. The ciphertext starts after it.
pub const HEADER_LEN: u64 = 4096;

/// The default length of the plaintext of a chunk, in bytes. Every chunk is
/// encrypted and authenticated independently, so a range of a piece is read by
/// decrypting only the chunks covering it.
pub const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024;

/// The length of the AES-256-GCM nonce stored in front of every chunk, in bytes.
pub const NONCE_LEN: u64 = 12;

/// The length of the AES-256-GCM authentication tag stored after every chunk, in
/// bytes.
pub const TAG_LEN: u64 = 16;

/// The number of bytes a chunk grows by when encrypted: the nonce and the tag.
pub const CHUNK_OVERHEAD: u64 = NONCE_LEN + TAG_LEN;

/// Layout maps the plaintext of a task to its ciphertext on disk.
///
/// The content is split into pieces of `piece_length` (the last piece may be
/// shorter), exactly as the plaintext content is, and every piece is split into
/// chunks of `chunk_size` (the last chunk of a piece may be shorter). Every
/// chunk is stored as `nonce || ciphertext || tag`, so a chunk takes
/// `chunk_size + CHUNK_OVERHEAD` bytes on disk. The pieces are stored one after
/// another at a fixed stride, so the pieces can be written concurrently and
/// out of order into disjoint regions, and any chunk is located from its
/// plaintext offset alone.
///
/// ```text
/// +--------+------------------------------+------------------------------+-----
/// | header | piece 0                      | piece 1                      | ...
/// |        | chunk 0  | chunk 1  | ...    | chunk 0  | chunk 1  | ...    |
/// |        | n|ct|tag | n|ct|tag |        | n|ct|tag | n|ct|tag |        |
/// +--------+------------------------------+------------------------------+-----
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    /// The length of the plaintext of a chunk.
    pub chunk_size: u64,

    /// The length of the plaintext of a piece.
    pub piece_length: u64,

    /// The length of the plaintext content.
    pub content_length: u64,
}

/// PieceLayout is a piece of the content, validated against the layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceLayout {
    /// The number of the piece.
    pub number: u64,

    /// The offset of the piece in the plaintext content.
    pub offset: u64,

    /// The length of the plaintext of the piece.
    pub length: u64,
}

/// Implements the layout.
impl Layout {
    /// Creates a new layout, validating its parameters.
    pub fn new(chunk_size: u64, piece_length: u64, content_length: u64) -> Result<Self> {
        if chunk_size == 0 {
            return Err(Error::Encryption("chunk size must not be zero".to_string()));
        }

        if content_length > 0 && piece_length == 0 {
            return Err(Error::Encryption(
                "piece length must not be zero for a non-empty content".to_string(),
            ));
        }

        Ok(Self {
            chunk_size,
            piece_length,
            content_length,
        })
    }

    /// Returns the number of chunks of a plaintext of `length` bytes.
    pub fn chunk_count(&self, length: u64) -> u64 {
        length.div_ceil(self.chunk_size)
    }

    /// Returns the length on disk of a plaintext of `length` bytes stored as
    /// consecutive chunks.
    pub fn encrypted_length(&self, length: u64) -> u64 {
        length + self.chunk_count(length) * CHUNK_OVERHEAD
    }

    /// Returns the distance on disk between the starts of two consecutive
    /// pieces.
    pub fn piece_stride(&self) -> u64 {
        self.encrypted_length(self.piece_length)
    }

    /// Returns the number of pieces of the content.
    pub fn piece_count(&self) -> u64 {
        if self.content_length == 0 {
            return 0;
        }

        self.content_length.div_ceil(self.piece_length)
    }

    /// Returns the length of the encrypted file on disk, including the header.
    pub fn file_length(&self) -> u64 {
        let pieces = self.piece_count();
        if pieces == 0 {
            return HEADER_LEN;
        }

        let last_piece_length = self.content_length - (pieces - 1) * self.piece_length;
        HEADER_LEN + (pieces - 1) * self.piece_stride() + self.encrypted_length(last_piece_length)
    }

    /// Validates that the plaintext range `[offset, offset + length)` is a piece
    /// of the content, or a prefix of one, and returns the piece.
    pub fn piece(&self, offset: u64, length: u64) -> Result<PieceLayout> {
        let number = if self.piece_length == 0 {
            0
        } else {
            offset / self.piece_length
        };

        if number * self.piece_length != offset
            || length > self.piece_length
            || offset
                .checked_add(length)
                .is_none_or(|end| end > self.content_length)
        {
            return Err(Error::Encryption(format!(
                "range offset {} length {} is not a piece of the content: piece length {}, content length {}",
                offset, length, self.piece_length, self.content_length
            )));
        }

        Ok(PieceLayout {
            number,
            offset,
            length,
        })
    }

    /// Returns the length of the plaintext of the chunk `index` of a piece of
    /// `piece_length` bytes.
    pub fn chunk_length(&self, piece_length: u64, index: u64) -> u64 {
        piece_length
            .saturating_sub(index * self.chunk_size)
            .min(self.chunk_size)
    }

    /// Returns the offset on disk of the chunk `index` of the piece.
    pub fn chunk_offset(&self, piece: &PieceLayout, index: u64) -> u64 {
        HEADER_LEN + piece.number * self.piece_stride() + index * (self.chunk_size + CHUNK_OVERHEAD)
    }

    /// Returns the plaintext offset of the chunk `index` of the piece, which is
    /// bound to the chunk as the associated data of its encryption.
    pub fn chunk_plaintext_offset(&self, piece: &PieceLayout, index: u64) -> u64 {
        piece.offset + index * self.chunk_size
    }

    /// Returns the offset on disk and the length on disk of the piece at
    /// `offset` with `length` bytes, i.e. the ciphertext of all its chunks.
    pub fn piece_disk_range(&self, offset: u64, length: u64) -> Result<(u64, u64)> {
        let piece = self.piece(offset, length)?;
        Ok(self.chunks_span(&piece, 0, self.chunk_count(length)))
    }

    /// Returns the offset on disk and the length on disk of the chunks
    /// `[first, first + count)` of the piece.
    pub fn chunks_span(&self, piece: &PieceLayout, first: u64, count: u64) -> (u64, u64) {
        let length = (first..first + count)
            .map(|index| self.chunk_length(piece.length, index) + CHUNK_OVERHEAD)
            .sum();
        (self.chunk_offset(piece, first), length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout_new() {
        assert!(Layout::new(0, 8, 16).is_err());
        assert!(Layout::new(4, 0, 16).is_err());
        assert!(Layout::new(4, 0, 0).is_ok());
        assert!(Layout::new(4, 8, 16).is_ok());
    }

    #[test]
    fn test_layout_lengths() {
        let layout = Layout::new(4, 10, 25).unwrap();
        assert_eq!(layout.chunk_count(0), 0);
        assert_eq!(layout.chunk_count(4), 1);
        assert_eq!(layout.chunk_count(5), 2);
        assert_eq!(layout.encrypted_length(10), 10 + 3 * CHUNK_OVERHEAD);
        assert_eq!(layout.piece_stride(), 10 + 3 * CHUNK_OVERHEAD);
        assert_eq!(layout.piece_count(), 3);

        // Two full pieces of three chunks and a last piece of two chunks.
        assert_eq!(
            layout.file_length(),
            HEADER_LEN + 2 * (10 + 3 * CHUNK_OVERHEAD) + 5 + 2 * CHUNK_OVERHEAD
        );

        let empty = Layout::new(4, 0, 0).unwrap();
        assert_eq!(empty.piece_count(), 0);
        assert_eq!(empty.file_length(), HEADER_LEN);
    }

    #[test]
    fn test_layout_piece() {
        let layout = Layout::new(4, 10, 25).unwrap();
        assert_eq!(
            layout.piece(0, 10).unwrap(),
            PieceLayout {
                number: 0,
                offset: 0,
                length: 10
            }
        );
        assert_eq!(layout.piece(20, 5).unwrap().number, 2);

        // A prefix of a piece is accepted.
        assert_eq!(layout.piece(10, 3).unwrap().number, 1);

        // Unaligned, too long and out of bounds ranges are rejected.
        assert!(layout.piece(5, 5).is_err());
        assert!(layout.piece(0, 11).is_err());
        assert!(layout.piece(20, 6).is_err());
        assert!(layout.piece(30, 1).is_err());
        assert!(layout.piece(u64::MAX - 1, 10).is_err());

        let empty = Layout::new(4, 0, 0).unwrap();
        assert_eq!(empty.piece(0, 0).unwrap().number, 0);
        assert!(empty.piece(0, 1).is_err());
    }

    #[test]
    fn test_layout_chunks() {
        let layout = Layout::new(4, 10, 25).unwrap();
        assert_eq!(layout.chunk_length(10, 0), 4);
        assert_eq!(layout.chunk_length(10, 2), 2);
        assert_eq!(layout.chunk_length(10, 3), 0);
        assert_eq!(layout.chunk_length(5, 1), 1);

        let piece = layout.piece(10, 10).unwrap();
        assert_eq!(
            layout.chunk_offset(&piece, 0),
            HEADER_LEN + layout.piece_stride()
        );
        assert_eq!(
            layout.chunk_offset(&piece, 2),
            HEADER_LEN + layout.piece_stride() + 2 * (4 + CHUNK_OVERHEAD)
        );
        assert_eq!(layout.chunk_plaintext_offset(&piece, 2), 18);

        let (offset, length) = layout.chunks_span(&piece, 1, 2);
        assert_eq!(
            offset,
            HEADER_LEN + layout.piece_stride() + (4 + CHUNK_OVERHEAD)
        );
        assert_eq!(length, (4 + CHUNK_OVERHEAD) + (2 + CHUNK_OVERHEAD));

        // The disk range of a piece covers all its chunks.
        assert_eq!(
            layout.piece_disk_range(10, 10).unwrap(),
            layout.chunks_span(&piece, 0, 3)
        );
        assert_eq!(
            layout.piece_disk_range(20, 5).unwrap().1,
            5 + 2 * CHUNK_OVERHEAD
        );
        assert!(layout.piece_disk_range(5, 5).is_err());

        // The pieces never overlap on disk.
        let next = layout.piece(20, 5).unwrap();
        assert_eq!(
            layout.chunk_offset(&next, 0),
            layout.chunk_offset(&piece, 0) + layout.encrypted_length(10)
        );
    }
}
