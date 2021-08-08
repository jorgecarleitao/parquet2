use futures::pin_mut;
use futures::StreamExt;
use parquet2::error::Result;
use parquet2::read::get_page_stream;
use parquet2::read::read_metadata_async;
use parquet2::statistics::BinaryStatistics;
use s3::Bucket;

mod stream;
use stream::RangedStreamer;

#[tokio::main]
async fn main() -> Result<()> {
    let bucket_name = "ursa-labs-taxi-data";
    let region = "us-east-2".parse().unwrap();
    let bucket = Bucket::new_public(bucket_name, region).unwrap();
    let path = "2009/01/data.parquet".to_string();

    let (data, _) = bucket.head_object(&path).await.unwrap();
    let length = data.content_length.unwrap() as usize;

    let mut reader = RangedStreamer::new(length, bucket, path, 1024 * 1024);

    let metadata = read_metadata_async(&mut reader).await?;

    // metadata
    println!("{}", metadata.num_rows);

    // pages of the first row group and first column
    let pages = get_page_stream(&metadata, 0, 0, &mut reader, vec![]).await?;

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
