use std::fs::File;
use std::io::Write;
use tempfile::tempfile;

use criterion::{criterion_group, criterion_main, Criterion};

use buffered_offset_reader::*;

fn make_temp_file(chunk_size: u8, chunk_count: u64) -> (File, Vec<u8>) {
    let mut file = tempfile().unwrap();
    let chunk = (0..chunk_size).into_iter().collect::<Vec<u8>>();

    for _ in 0..chunk_count {
        file.write(&chunk).unwrap();
    }
    (file, chunk)
}

const CHUNK_SIZE: u8 = 64;
const CHUNK_COUNT: u64 = 1024;

fn read_file(c: &mut Criterion) {
    let (file, chunk) = make_temp_file(CHUNK_SIZE, CHUNK_COUNT);

    c.bench_function("file_read_at", move |b| {
        let mut tmp = vec![0; CHUNK_SIZE as usize];
        b.iter(|| {
            for i in 0..CHUNK_COUNT {
                file.read_at(&mut tmp, i * CHUNK_SIZE as u64).unwrap();
                assert_eq!(&tmp, &chunk);
            }
        });
    });
}

fn read_buffered(c: &mut Criterion) {
    let (file, chunk) = make_temp_file(CHUNK_SIZE, CHUNK_COUNT);

    c.bench_function("buffered_read_at", move |b| {
        let mut r = BufOffsetReader::with_capacity(CHUNK_SIZE as usize * 16, file.try_clone().unwrap());

        let mut tmp = vec![0; CHUNK_SIZE as usize];
        b.iter(|| {
            for i in 0..CHUNK_COUNT {
                r.read_at(&mut tmp, i * CHUNK_SIZE as u64).unwrap();
                assert_eq!(&tmp, &chunk);
            }
        });
    });
}

criterion_group!(benches, read_file, read_buffered);
criterion_main!(benches);
