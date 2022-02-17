use crate::error::Result;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};
use parquet_format_async_temp::thrift::protocol::{
    TCompactInputProtocol, TCompactInputStreamProtocol, TCompactOutputProtocol,
    TCompactOutputStreamProtocol, TOutputProtocol, TOutputStreamProtocol,
};
use parquet_format_async_temp::{ColumnChunk, FileMetaData, PageHeader};
use std::io::{Read, Write};

#[async_trait]
pub trait ThriftReader: Sized {
    fn read_thrift_from<R: Read>(reader: &mut R) -> Result<Self>;
    async fn read_thrift_from_async<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<Self>;
}

#[async_trait]
pub trait ThriftWriter: Sized {
    fn write_thrift_to<W: Write>(&self, writer: &mut W) -> Result<usize>;
    async fn write_thrift_to_async<W: AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> Result<usize>;
}

macro_rules! define_thrift_impl {
    ( $typ:ty ) => {
        #[async_trait]
        impl ThriftReader for $typ {
            fn read_thrift_from<R: Read>(reader: &mut R) -> Result<Self> {
                let mut protocol = TCompactInputProtocol::new(reader);
                let value = <$typ>::read_from_in_protocol(&mut protocol)?;
                Ok(value)
            }
            async fn read_thrift_from_async<R: AsyncRead + Unpin + Send>(
                reader: &mut R,
            ) -> Result<Self> {
                let mut prot = TCompactInputStreamProtocol::new(reader);
                Ok(<$typ>::stream_from_in_protocol(&mut prot).await?)
            }
        }

        #[async_trait]
        impl ThriftWriter for $typ {
            async fn write_thrift_to_async<W: AsyncWrite + Unpin + Send>(
                &self,
                writer: &mut W,
            ) -> Result<usize> {
                let mut protocol = TCompactOutputStreamProtocol::new(writer);
                let n = self.write_to_out_stream_protocol(&mut protocol).await?;
                protocol.flush().await?;
                Ok(n)
            }

            fn write_thrift_to<W: Write>(&self, writer: &mut W) -> Result<usize> {
                let mut protocol = TCompactOutputProtocol::new(writer);
                let n = self.write_to_out_protocol(&mut protocol)?;
                protocol.flush()?;
                Ok(n)
            }
        }
    };
}

define_thrift_impl!(PageHeader);
define_thrift_impl!(ColumnChunk);
define_thrift_impl!(FileMetaData);
