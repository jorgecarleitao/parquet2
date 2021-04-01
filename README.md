# Parquet2

This is an experiment, a re-write of the parquet crate that
* delegates parallelism downstream
* deletages decoding batches downstream
* no `unsafe`

## Organization

* `read`: read metadata and pages
* `metadata`: parquet files metadata (e.g. `FileMetaData`)
* `schema`: types metadata declaration (e.g. `ConvertedType`)
* `types`: physical type declaration (i.e. how things are represented in memory). So far unused.
* `compression`: compression (e.g. Gzip)
* `errors`: basic error handling

## How to use

```rust
use std::fs::File;

use parquet2::read::{Page, read_metadata, get_page_iterator};

let mut file = File::open("testing/parquet-testing/data/alltypes_plain.parquet").unwrap();

/// here we read the metadata.
let metadata = read_metadata(&mut file)?;

/// Here we get an iterator of pages (each page has its own data)
/// This can be heavily parallelized; not even the same `file` is needed here...
/// feel free to wrap `metadata` under an `Arc`
let row_group = 0;
let column = 0;
let mut iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

/// A page. It is just (encoded) bytes at this point.
let page = iter.next().unwrap().unwrap();
println!("{:#?}", page);
```

## General data flow

`parquet -> decompress -> decode -> deserialize`

* `decompress`: e.g. `gzip`
* `decode`: e.g. `RLE`
* `deserialize`: e.g. `&[u8] -> &[i32]`

Decoding and deserialization are still in progress.

### Decoding ideas:

See https://github.com/tantivy-search/bitpacking/issues/31

### Deserialization ideas:

Since decoding may be the final step of the pipeline, a decoded page may require type information.

This requires consumer-specific knowledge (e.g. arrow may use buffers while non-arrow may use `Vec`). Thus, IMO we should just offer the APIs and decoders, and leave it for consumers to 
use them.

For dictionary-encoded pages, we should only have to decode the keys once. However, doing so requires an in-memory representation.
