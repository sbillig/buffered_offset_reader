use std::fs::File;
use std::io;

mod range;
use range::*;

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub trait OffsetRead {
    fn read_at(&self, buf: &mut [u8], offset: usize) -> io::Result<usize>;
}

pub trait OffsetReadMut {
    fn read_at(&mut self, buf: &mut [u8], offset: usize) -> io::Result<usize>;
}

// The `BufOffsetReader` struct is like `std::io::BufReader`,
// but uses `read_at()` (aka `pread()`) instead of `read()`.
pub struct BufOffsetReader<'a, R: OffsetRead> {
    inner: &'a R,
    range: Range,
    buffer: Vec<u8>,
}

impl<'a, R: OffsetRead> BufOffsetReader<'a, R> {
    pub fn new(inner: &'a R) -> BufOffsetReader<'a, R> {
        BufOffsetReader::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(cap: usize, inner: &'a R) -> BufOffsetReader<'a, R> {
        BufOffsetReader {
            inner,
            range: 0..0,
            buffer: vec![0; cap],
        }
    }

    pub fn contains(&self, r: Range) -> bool {
        self.range.intersect(&r) == r
    }

    fn load_page_at_offset(&mut self, offset: usize) -> io::Result<usize> {
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

impl<'a, R: OffsetRead> OffsetReadMut for BufOffsetReader<'a, R> {
    fn read_at(&mut self, mut buf: &mut [u8], offset: usize) -> io::Result<usize> {
        let r = offset..(offset + buf.len());
        let mut i = self.range.intersect(&r);
        if i.len() < buf.len() {
            self.load_page_at_offset(offset)?;
            i = self.range.intersect(&r)
        }
        self.copy_range_to_slice(&i, &mut buf);
        Ok(i.len())
    }
}

impl OffsetRead for File {
    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: usize) -> io::Result<usize> {
        use std::os::unix::prelude::FileExt;
        FileExt::read_at(self, buf, offset as u64)
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: usize) -> io::Result<usize> {
        use std::os::windows::prelude::FileExt;
        FileExt::seek_read(self, buf, offset as u64)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::io::Write;
    use tempfile::tempfile;

    #[test]
    fn buffered_read_at() {
        let v = (0..200).into_iter().collect::<Vec<u8>>();

        let mut file = tempfile().unwrap();

        file.write(&v).unwrap();

        let mut tmp = vec![111; 4];
        let mut r = BufOffsetReader::with_capacity(64, &file);

        r.read_at(&mut tmp, 0).unwrap();
        assert_eq!(&tmp, &[0, 1, 2, 3]);

        assert!(r.contains(40..50));
        assert!(!r.contains(66..70));

        r.read_at(&mut tmp, 65).unwrap();
        assert_eq!(&tmp, &[65, 66, 67, 68]);
        assert!(r.contains(70..74));

        r.read_at(&mut tmp, 70).unwrap();
        assert_eq!(&tmp, &[70, 71, 72, 73]);

        assert!(!r.contains(0..4));
        r.read_at(&mut tmp, 0).unwrap();
        assert_eq!(&tmp, &[0, 1, 2, 3]);

        let rlen = r.read_at(&mut tmp, 197).unwrap();
        assert_eq!(rlen, 3);
        assert_eq!(&tmp[0..3], &[197, 198, 199]);

        let rlen = r.read_at(&mut tmp, 200).unwrap();
        assert_eq!(rlen, 0);
    }

    fn do_reads<F>(mut read_at: F)
    where
        F: FnMut(&mut [u8], usize) -> io::Result<usize>,
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

        let mut reader = BufOffsetReader::with_capacity(64, &file);
        do_reads(|b, o| reader.read_at(b, o));
    }
}
