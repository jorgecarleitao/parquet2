use crate::error::Result;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};
use parquet_format_async_temp::thrift::protocol::{
    TCompactInputProtocol, TCompactInputStreamProtocol, TCompactOutputProtocol,
    TCompactOutputStreamProtocol, TInputProtocol, TInputStreamProtocol, TOutputProtocol,
    TOutputStreamProtocol,
};
use parquet_format_async_temp::thrift::Result as ThriftResult;
use parquet_format_async_temp::{ColumnChunk, FileMetaData, PageHeader};
use std::io::{Read, Write};

#[async_trait]
pub trait ThriftType: Sized {
    fn write_to_out_protocol(&self, o_prot: &mut dyn TOutputProtocol) -> ThriftResult<usize>;

    async fn write_to_out_stream_protocol(
        &self,
        o_prot: &mut dyn TOutputStreamProtocol,
    ) -> ThriftResult<usize>;

    fn read_from_in_protocol(i_prot: &mut dyn TInputProtocol) -> ThriftResult<Self>;

    async fn stream_from_in_protocol(i_prot: &mut dyn TInputStreamProtocol) -> ThriftResult<Self>;
}

macro_rules! define_thrift_impl {
    ( $type:ty ) => {
        #[async_trait]
        impl ThriftType for $type {
            fn write_to_out_protocol(
                &self,
                o_prot: &mut dyn TOutputProtocol,
            ) -> ThriftResult<usize> {
                self.write_to_out_protocol(o_prot)
            }

            async fn write_to_out_stream_protocol(
                &self,
                o_prot: &mut dyn TOutputStreamProtocol,
            ) -> ThriftResult<usize> {
                self.write_to_out_stream_protocol(o_prot).await
            }

            fn read_from_in_protocol(i_prot: &mut dyn TInputProtocol) -> ThriftResult<Self> {
                <$type>::read_from_in_protocol(i_prot)
            }

            async fn stream_from_in_protocol(
                i_prot: &mut dyn TInputStreamProtocol,
            ) -> ThriftResult<Self> {
                <$type>::stream_from_in_protocol(i_prot).await
            }
        }
    };
}

define_thrift_impl!(PageHeader);
define_thrift_impl!(ColumnChunk);
define_thrift_impl!(FileMetaData);

pub fn read_from_thrift<T: ThriftType, R: Read>(reader: &mut R) -> Result<T> {
    let mut protocol = TCompactInputProtocol::new(reader);
    let value = T::read_from_in_protocol(&mut protocol)?;
    Ok(value)
}

pub async fn read_from_thrift_async<T: ThriftType, R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<T> {
    let mut prot = TCompactInputStreamProtocol::new(reader);
    Ok(T::stream_from_in_protocol(&mut prot).await?)
}

pub fn write_to_thrift<T: ThriftType, W: Write>(value: &T, writer: &mut W) -> Result<usize> {
    let mut protocol = TCompactOutputProtocol::new(writer);
    let r = value.write_to_out_protocol(&mut protocol)?;
    protocol.flush()?;
    Ok(r)
}

pub async fn write_to_thrift_async<T: ThriftType, W: AsyncWrite + Unpin + Send>(
    value: &T,
    writer: &mut W,
) -> Result<usize> {
    let mut protocol = TCompactOutputStreamProtocol::new(writer);
    let r = value.write_to_out_stream_protocol(&mut protocol).await?;
    protocol.flush().await?;
    Ok(r)
}
