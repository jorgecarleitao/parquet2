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

Since decoding may be the final step of the pipeline (e.g. when we decode a bitmap), a decoded
page may require type information. Idea:

```rust
use std::any::Any;

use crate::schema::types::PhysicalType;

pub trait DecodedPage {
    fn as_any(&self) -> &dyn Any;
    fn physical_type(&self) -> &PhysicalType;
    fn rep_levels(&self) -> &[i32];
    fn def_levels(&self) -> &[i32];
}

use std::sync::Arc;

use crate::{
    errors::Result,
    read::{
        decoded_page::DecodedPage,
        page::{PageDict, PageV1, PageV2},
    },
    metadata::ColumnDescriptor,
    read::page::Page,
};

// own page so that we can re-use its buffer
fn read_page(page: Page, descriptor: &ColumnDescriptor) -> Result<Arc<dyn DecodedPage>> {
    match page {
        Page::V1(page) => read_page_v1(page, descriptor),
        Page::V2(page) => read_page_v2(page, descriptor),
        Page::Dictionary(page) => read_page_dictionary(page, descriptor),
    }
}

fn read_page_v1(page: PageV1, descriptor: &ColumnDescriptor) -> Result<Arc<dyn DecodedPage>> {
    todo!()
}

fn read_page_v2(page: PageV2, descriptor: &ColumnDescriptor) -> Result<Arc<dyn DecodedPage>> {
    todo!()
}

fn read_page_dictionary(
    page: PageDict,
    descriptor: &ColumnDescriptor,
) -> Result<Arc<dyn DecodedPage>> {
    todo!()
}
```

Alternative: concatenate pages together in a single run, since pages are read by a single reader  (because they are sequential).

Regardless, this requires consumer-specific knowledge (e.g. arrow may use buffers while non-arrow may use `Vec`). Thus, IMO we should just offer the APIs and decoders, and leave it for consumers to 
use them.
