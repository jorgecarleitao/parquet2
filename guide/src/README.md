# Parquet2

Parquet2 is a rust library to interact with the
[parquet format](https://en.wikipedia.org/wiki/Apache_Parquet), welcome to its guide!

This guide describes on how to efficiently and safely read and write to and from parquet.
Before starting, there are two concepts to introduce in the context of this guide:

* IO-bound operations: perform either disk reads or network calls (e.g. s3)
* CPU-bound operations: perform compute

In this guide, "read", "write" and "seek" correspond to
IO-bound operations, "decompress", "compress", "deserialize", etc. are CPU-bound.

Generally, IO-bound operations are parallelizable with green threads, while CPU-bound
operations are not. 

## Metadata

The starting point of reading a parquet file is reading its metadata (at the end of the file).
To do so, we offer two functions, `parquet2::read::read_metadata`, for sync reads:

#### Sync

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:metadata}}
```

#### Async

and `parquet2::read::read_metadata_async`, for async reads (using `tokio::fs` as example):

```rust
{{#include ../../examples/read_metadata_async/src/main.rs}}
```

In both cases, `metadata: FileMetaData` is the file's metadata.

## Columns, Row Groups, Columns chunks and Pages

At this point, it is important to give a small introduction to the format itself.

The metadata does not contain any data. Instead, the metadata contains
the necessary information to read, decompress, decode and deserialize data. Generally:

* a file has a schema with columns and data
* data in the file is divided in _row groups_
* each _row group_ contains _column chunks_
* each _column chunk_ contains _pages_
* each _page_ contains multiple values

each of the entities above has associated metadata. Except for the pages,
all this metadata is already available in the `FileMetaData`.
Here we will focus on a single column to show how we can read it.

We access the metadata of a column chunk via

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:column_metadata}}
```

From this, we can produce an iterator of compressed pages (sync), 
`parquet2::read::get_page_iterator` or a stream (async) of compressed
pages, `parquet2::read::get_page_stream`:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:pages}}
```

in both cases they yield individual `CompressedDataPage`s (values between
pages are part of the same array).

At this point, we are missing 3 steps: decompress, decode and deserialize.
Decompressing is done via `decompress`:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:decompress}}
```

Decoding and deserialization is usually done in the same step as follows:

```rust,no_run,noplayground
{{#include ../../examples/read_metadata.rs:deserialize}}
```

the details of the `todo!` are highly specific to the target in-memory format to use.
Thus, here we only describe how to decompose the page buffer in its individual
components and what you need to worry about. Refer to the integration tests's
implementation for deserialization to a simple in-memory format,
and [arrow2](https://github.com/jorgecarleitao/arrow2)'s
implementation for the Apache Arrow format.
