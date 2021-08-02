use parquet2::read::read_metadata_async;
use s3::Bucket;

mod stream;
use stream::RangedStreamer;

#[tokio::main]
async fn main() {
    let bucket_name = "ursa-labs-taxi-data";
    let region = "us-east-2".parse().unwrap();
    let bucket = Bucket::new_public(bucket_name, region).unwrap();
    let path = "2009/01/data.parquet".to_string();

    let (data, _) = bucket.head_object(&path).await.unwrap();
    let length = data.content_length.unwrap() as usize;

    let mut reader = RangedStreamer::new(length, bucket, path, 1024 * 1024);

    let metadata = read_metadata_async(&mut reader).await.unwrap();

    println!("{}", metadata.num_rows);
}
