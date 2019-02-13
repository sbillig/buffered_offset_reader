# buffered\_offset\_reader

[![Build Status](https://travis-ci.org/sbillig/buffered_offset_reader.svg?branch=master)](https://travis-ci.org/sbillig/buffered_offset_reader)
[![Documentation](https://docs.rs/buffered_offset_reader/badge.svg)](https://docs.rs/buffered_offset_reader)

Rust crate for buffered reading of a file at arbitrary offsets.

Uses `std::os::unix::fs::FileExt::read_at()` on unix (aka `pread()`)
or `std::os::windows::fs::FileExt::seek_read()` on windows to do thread-safe
reads of the underlying file.

## Examples

```rust
use buffered_offset_reader::{BufOffsetReader, OffsetReadMut};
use std::fs::File;

fn main() -> std::io::Result<()> {
    let f = File::open("log.txt")?;
    let mut reader = BufOffsetReader::new(f);
    let mut tmp = vec![0; 8];

    reader.read_at(&mut tmp, 0)?;  // read 8 bytes at offset 0

	// This read will be fulfilled by the reader's internal buffer,
	// so it won't require a system call.
    reader.read_at(&mut tmp, 32)?; // read 8 bytes at offset 32

    Ok(())
}
```
