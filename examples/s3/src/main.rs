use std::sync::Arc;

use futures::{
    future::BoxFuture,
    pin_mut,
    StreamExt
};
use parquet2::{
    error::Result,
    read::{get_page_stream, read_metadata_async},
    statistics::BinaryStatistics,
};
use s3::Bucket;

mod stream;
use stream::{RangedStreamer, SeekOutput};

#[tokio::main]
async fn main() -> Result<()> {
    let bucket_name = "ursa-labs-taxi-data";
    let region = "us-east-2".parse().unwrap();
    let bucket = Bucket::new_public(bucket_name, region).unwrap();
    let path = "2009/01/data.parquet".to_string();

    let (data, _) = bucket.head_object(&path).await.unwrap();
    let length = data.content_length.unwrap() as usize;

    let range_get = std::sync::Arc::new(move |start: u64, length: usize| {
        let bucket = bucket.clone();
        let path = path.clone();
        Box::pin(async move {
            let bucket = bucket.clone();
            let path = path.clone();
            let (mut data, _) = bucket
                .get_object_range(&path, start, Some(start + length as u64))
                .await
                .map_err(|x| std::io::Error::new(std::io::ErrorKind::Other, x.to_string()))?;

            data.truncate(length);
            Ok(SeekOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<SeekOutput>>
    });

    let mut reader = RangedStreamer::new(length, 1024 * 1024, range_get);

    let metadata = read_metadata_async(&mut reader).await?;

    // metadata
    println!("{}", metadata.num_rows);

    // pages of the first row group and first column
    let pages = get_page_stream(&metadata, 0, 0, &mut reader, vec![], Arc::new(|_, _| true)).await?;

    pin_mut!(pages); // needed for iteration

    let first_page = pages.next().await.unwrap()?;
    // the page statistics
    // first unwrap: they exist
    let a = first_page.statistics().unwrap()?;
    let a = a.as_any().downcast_ref::<BinaryStatistics>().unwrap();
    println!("{:?}", a.min_value);
    println!("{:?}", a.max_value);
    println!("{:?}", a.null_count);
    Ok(())
}
