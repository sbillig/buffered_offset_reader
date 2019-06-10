#![forbid(unsafe_code)]
//! `BufOffsetReader` is like `std::io::BufReader`,
//! but it allows reading at arbitrary positions in the underlying file.
//!
//! Uses `std::os::unix::fs::FileExt::read_at()` on unix (aka `pread()`)
//! and `std::os::windows::fs::FileExt::seek_read()` on windows to read
//! from the underlying file in a thread-safe manner, so only a non-mutable reference
//! to the file is needed.
//!
//! # Examples
//!
//! ```no_run
//! use buffered_offset_reader::{BufOffsetReader, OffsetReadMut};
//! use std::fs::File;
//!
//! fn main() -> std::io::Result<()> {
//!     let f = File::open("log.txt")?;
//!     let mut r = BufOffsetReader::new(f);
//!     let mut buf = vec![0; 8];
//!
//!     r.read_at(&mut buf, 0)?;  // read 8 bytes at offset 0
//!     r.read_at(&mut buf, 32)?; // read 8 bytes at offset 32
//!     Ok(())
//! }
//! ```
//!
//! NB: The buffering logic is currently very simple: if the requested
//! range isn't completely contained in the buffer, we read `capacity` bytes
//! into memory, starting at the requested offset. This works well for generally
//! "forward" reads, but not so great for eg. iterating backward through a file.

use std::cmp::min;
use std::fs::File;
use std::io;

mod range;
use range::*;

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub trait OffsetRead {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
}

pub trait OffsetReadMut {
    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
}

pub struct BufOffsetReader<R: OffsetRead> {
    inner: R,
    range: Range,
    buffer: Vec<u8>,
}

impl<R: OffsetRead> BufOffsetReader<R> {
    /// Creates a new buffered reader with default buffer capacity (currently 8KB).
    pub fn new(inner: R) -> BufOffsetReader<R> {
        BufOffsetReader::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(cap: usize, inner: R) -> BufOffsetReader<R> {
        BufOffsetReader {
            inner,
            range: 0..0,
            buffer: vec![0; cap],
        }
    }

    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Check whether the specified data range (of the underlying file) is
    /// currently contained in the reader's in-memory buffer.
    pub fn contains(&self, r: Range) -> bool {
        self.range.intersect(&r) == r
    }

    pub fn clear(&mut self) {
        self.range = 0..0;
    }

    fn load_page_at_offset(&mut self, offset: u64) -> io::Result<usize> {
        let count = self.inner.read_at(&mut self.buffer, offset)?;
        self.range = (offset as usize)..(offset as usize + count);
        Ok(count)
    }

    fn copy_range_to_slice(&self, r: &Range, buf: &mut [u8]) {
        if r.len() > 0 {
            let src = r.shift_left(self.range.start);
            let dst = r.shift_left(r.start);
            buf[dst].copy_from_slice(&self.buffer[src]);
        }
    }
}

impl<R: OffsetRead> OffsetReadMut for BufOffsetReader<R> {
    fn read_at(&mut self, mut buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if buf.len() > self.capacity() {
            return self.inner.read_at(&mut buf, offset);
        }

        let r = (offset as usize)..(offset as usize + buf.len());
        let mut i = self.range.intersect(&r);

        if i.len() < buf.len() {
            self.load_page_at_offset(offset)?;
            i = self.range.intersect(&r)
        }
        self.copy_range_to_slice(&i, &mut buf);
        Ok(i.len())
    }
}

impl OffsetRead for &[u8] {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        Ok(self.get(offset as usize..).map_or(0, |r| {
            let n = min(r.len(), buf.len());
            buf[..n].copy_from_slice(&r[..n]);
            n
        }))
    }
}

impl OffsetRead for File {
    /// Uses `std::os::unix::fs::FileExt::read_at()` (aka `pread()`) on unix
    /// and `std::os::windows::fs::FileExt::seek_read()` on windows.
    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::prelude::FileExt;
        FileExt::read_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::prelude::FileExt;
        FileExt::seek_read(self, buf, offset)
    }
}

pub trait OffsetWrite {
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize>;
}

impl OffsetWrite for File {
    /// For convenience, we also expose write_at (for File), because
    /// code that needs to read_at might want to write_at.
    ///
    /// Uses `std::os::unix::prelude::FileExt::write_at` and
    /// `std::os::windows::prelude::FileExt::seek_write`.
    #[cfg(unix)]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::prelude::FileExt;
        FileExt::write_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::prelude::FileExt;
        FileExt::seek_write(self, buf, offset)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::io::Write;
    use tempfile::tempfile;

    #[test]
    fn buffered_read_at() -> Result<(), io::Error> {
        let v = (0..200).into_iter().collect::<Vec<u8>>();

        let mut file = tempfile()?;

        file.write(&v)?;

        let mut tmp = vec![111; 4];
        let mut r = BufOffsetReader::with_capacity(64, file);

        r.read_at(&mut tmp, 0)?;
        assert_eq!(&tmp, &[0, 1, 2, 3]);

        assert!(r.contains(40..50));
        assert!(!r.contains(66..70));

        r.read_at(&mut tmp, 65)?;
        assert_eq!(&tmp, &[65, 66, 67, 68]);
        assert!(r.contains(70..74));

        r.read_at(&mut tmp, 70)?;
        assert_eq!(&tmp, &[70, 71, 72, 73]);

        assert!(!r.contains(0..4));
        r.read_at(&mut tmp, 0)?;
        assert_eq!(&tmp, &[0, 1, 2, 3]);

        // Read end of file
        let rlen = r.read_at(&mut tmp, 197)?;
        assert_eq!(rlen, 3);
        assert_eq!(&tmp[0..3], &[197, 198, 199]);

        // Read past the end of file
        let rlen = r.read_at(&mut tmp, 200)?;
        assert_eq!(rlen, 0);

        // Read more than the buffer capacity
        let mut bigtmp = vec![0; 100];
        let rlen = r.read_at(&mut bigtmp, 100)?;
        assert_eq!(rlen, 100);
        assert_eq!(&bigtmp[0..3], &[100, 101, 102]);

        Ok(())
    }

    #[test]
    fn read_and_write() -> Result<(), io::Error> {
        let v = (0..200).into_iter().collect::<Vec<u8>>();

        let file = tempfile()?;
        let mut r = BufOffsetReader::with_capacity(64, file.try_clone()?);

        file.write_at(&v, 0)?;
        let mut tmp = [0, 0, 0, 0];
        r.read_at(&mut tmp, 0)?;
        assert_eq!(&tmp, &[0, 1, 2, 3]);

        file.write_at(&[100, 100, 100, 100], 0)?;

        // r's buffer still contains the old values
        assert!(r.contains(0..4));
        r.read_at(&mut tmp, 0)?;
        assert_eq!(&tmp, &[0, 1, 2, 3]);

        r.clear();
        assert!(!r.contains(0..4));
        r.read_at(&mut tmp, 0)?;
        assert_eq!(&tmp, &[100, 100, 100, 100]);

        file.write_at(&v, 200)?;
        let c = r.read_at(&mut tmp, 210)?;
        assert_eq!(c, 4);
        assert_eq!(&tmp, &[10, 11, 12, 13]);
        Ok(())
    }

    #[test]
    fn slice_read_at() -> Result<(), io::Error> {
        let v = (0..200).into_iter().collect::<Vec<u8>>();
        let s = &v[..];

        let mut tmp = [0, 0, 0, 0];
        let n = s.read_at(&mut tmp, 100)?;
        assert_eq!(n, 4);
        assert_eq!(&tmp, &[100, 101, 102, 103]);

        let n = s.read_at(&mut tmp, 198)?;
        assert_eq!(n, 2);
        assert_eq!(&tmp, &[198, 199, 102, 103]);

        let n = s.read_at(&mut tmp, 300)?;
        assert_eq!(n, 0);
        assert_eq!(&tmp, &[198, 199, 102, 103]);

        Ok(())
    }

    fn do_reads<F>(mut read_at: F)
    where
        F: FnMut(&mut [u8], u64) -> io::Result<usize>,
    {
        let mut tmp = vec![0; 4];

        let r1 = read_at(&mut tmp, 0).unwrap();
        assert_eq!(r1, 4);
        assert_eq!(&tmp, &[0, 1, 2, 3]);

        let r2 = read_at(&mut tmp, 4).unwrap();
        assert_eq!(r2, 4);
        assert_eq!(&tmp, &[4, 5, 6, 7]);
    }

    #[test]
    fn generic_read_at() {
        let file: File = {
            let v = (0..255).into_iter().collect::<Vec<u8>>();
            let mut file = tempfile().unwrap();
            file.write(&v).unwrap();
            file
        };

        do_reads(|b, o| file.read_at(b, o));

        let mut reader = BufOffsetReader::with_capacity(64, file);
        do_reads(|b, o| reader.read_at(b, o));
    }
}
