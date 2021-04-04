extern crate parquet2;

use std::{fs::File, path::PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};

use parquet2::errors::Result;
use parquet2::read::{get_page_iterator, read_metadata};
use parquet2::serialization::arrow2::page_iter_to_array;

fn read_decompressed_pages(size: usize) -> Result<()> {
    // reads decompressed pages (i.e. CPU)
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(dir).join(format!("fixtures/pyarrow3/basic_nulls_{}.parquet", size));
    let mut file = File::open(path).unwrap();

    let metadata = read_metadata(&mut file)?;

    let row_group = 0;
    let column = 0;
    let iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

    let descriptor = &iter.descriptor().clone();
    let _ = page_iter_to_array(iter, descriptor)?;
    Ok(())
}

fn read_compressed_pages(size: usize) -> Result<()> {
    // reads compressed pages (i.e. no CPU)
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(dir).join(format!("fixtures/pyarrow3/basic_nulls_{}.parquet", size));
    let mut file = File::open(path).unwrap();

    let metadata = read_metadata(&mut file)?;

    let row_group = 0;
    let column = 0;
    let iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

    let _ = iter.collect::<Vec<_>>();
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("read_compressed_pages_10", |b| {
        b.iter(|| read_compressed_pages(10))
    });
    c.bench_function("read_compressed_pages_100", |b| {
        b.iter(|| read_compressed_pages(100))
    });
    c.bench_function("read_compressed_pages_1000", |b| {
        b.iter(|| read_compressed_pages(1000))
    });
    c.bench_function("read_compressed_pages_10000", |b| {
        b.iter(|| read_compressed_pages(10000))
    });

    c.bench_function("read_decompressed_pages_10", |b| {
        b.iter(|| read_decompressed_pages(10))
    });
    c.bench_function("read_decompressed_pages_100", |b| {
        b.iter(|| read_decompressed_pages(100))
    });
    c.bench_function("read_decompressed_pages_1000", |b| {
        b.iter(|| read_decompressed_pages(1000))
    });
    c.bench_function("read_decompressed_pages_10000", |b| {
        b.iter(|| read_decompressed_pages(10000))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
