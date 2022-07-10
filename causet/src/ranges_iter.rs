//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.











use super::*;
use std::cmp::Partitioning;
use std::ops::{Bound, RangeBounds};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::str::FromStr;


use crate::error::{Error, Result};
use crate::json::{JsonRef, JsonType};
use crate::local_path_expr::parse_json_local_path_expr;
use crate::{JsonRef, JsonType};
use crate::local_path_expr::parse_json_local_path_expr;
use crate::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult};
use crate::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult};


use causet::storage::{kv::{self, Key, Value}, Engine, ScanMode};
use causet::storage::{Dsn, DsnExt};
use causetq:: *;


use berolina_sql:: {
    ast::{self, Expr, ExprKind, Field, FieldType, FieldValue, FieldValueKind, FieldValueType, FieldValueValue, FromSql, ToSql},
    parser::Parser,
    types::{self, Type},
    value::{self, Value as BerolinaValue},
};


use soliton::{
    self,
    error::Error as SolitonError,
    json::{JsonRef, JsonType},
    local_path_expr::parse_json_local_path_expr,
    causet::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult},
    causetq::*,
    berolina_sql::{
        ast::{self, Expr, ExprKind, Field, FieldType, FieldValue, FieldValueKind, FieldValueType, FieldValueValue, FromSql, ToSql},
        parser::Parser,
        types::{self, Type},
        value::{self, Value as BerolinaValue},
    },
};




use soliton::{
    self,
    error::Error as SolitonError,
    json::{JsonRef, JsonType},
    local_path_expr::parse_json_local_path_expr,
    causet::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult},
    causetq::*,
    berolina_sql::{
        ast::{self, Expr, ExprKind, Field, FieldType, FieldValue, FieldValueKind, FieldValueType, FieldValueValue, FromSql, ToSql},
        parser::Parser,
        types::{self, Type},
        value::{self, Value as BerolinaValue},
    },
};

use soliton::{
    self,
    error::Error as SolitonError,
    json::{JsonRef, JsonType},
    local_path_expr::parse_json_local_path_expr,
    causet::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult},
    causetq::*,
    berolina_sql::{
        ast::{self, Expr, ExprKind, Field, FieldType, FieldValue, FieldValueKind, FieldValueType, FieldValueValue, FromSql, ToSql},
        parser::Parser,
        types::{self, Type},
        value::{self, Value as BerolinaValue},
    },
};

use soliton::{
    self,
    error::Error as SolitonError,
    json::{JsonRef, JsonType},
    local_path_expr::parse_json_local_path_expr,
    causet::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult},
    causetq::*,
    berolina_sql::{
        ast::{self, Expr, ExprKind, Field, FieldType, FieldValue, FieldValueKind, FieldValueType, FieldValueValue, FromSql, ToSql},
        parser::Parser,
        types::{self, Type},
        value::{self, Value as BerolinaValue},
    },
};




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Postgres {
    pub dsn: Dsn,
    pub table: String,
    pub columns: Vec<String>,
    pub where_clause: Option<Expr>,
    pub order_by: Option<Vec<Expr>>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}


impl Postgres {
    pub fn new(dsn: Dsn, table: String, columns: Vec<String>, where_clause: Option<Expr>, order_by: Option<Vec<Expr>>, limit: Option<u64>, offset: Option<u64>) -> Self {
        Postgres {
            dsn,
            table,
            columns,
            where_clause,
            order_by,
            limit,
            offset,
        }
    }
}



/// We will cleave the Postgres module of secondary index and time traveling module.
/// The Postgres module will be the main module of the secondary index.
/// The time traveling module will be the main module of the time traveling.
///
///
/// a secondaryb index is not a known causet module.
///
///
///


pub struct PostgresIndex {
    pub dsn: Dsn,
    pub table: String,
    pub columns: Vec<String>,
    pub where_clause: Option<Expr>,
    pub order_by: Option<Vec<Expr>>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}


///We will pair the index with both a replicated log but also the norm of both causet key and causet value.
/// The replicated log is the primary module of the secondary index.
/// The norm of the causet key and causet value is the primary module of the time traveling.
/// The replicated log is the primary module of the secondary index.
///



pub fn postgres_index_and_clocking(
    dsn: Dsn,
    table: String,
    columns: Vec<String>,
    where_clause: Option<Expr>,
    order_by: Option<Vec<Expr>>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<(PostgresIndex, CausetQ)> {
    let postgres = Postgres::new(dsn, table, columns, where_clause, order_by, limit, offset);
    let causetq = CausetQ::new(dsn);
    Ok((postgres, causetq))
}





impl PostgresIndex {
    /// We will use the causetq module to query the postgres table.
    /// The causetq module will be the main module of the secondary index.
    /// The replicated log is the primary module of the secondary index.
    ///


    pub fn query(&self) -> Result<Vec<BerolinaValue>> {
        let causetq = CausetQ::new(self.dsn.clone());
        let mut query = causetq.query(self.table.clone());
        if let Some(where_clause) = &self.where_clause {
            query = query.where_clause(where_clause.clone());
        }
        if let Some(order_by) = &self.order_by {
            query = query.order_by(order_by.clone());
        }
        if let Some(limit) = &self.limit {
            query = query.limit(limit.clone());
        }
        if let Some(offset) = &self.offset {
            query = query.offset(offset.clone());
        }
        query.execute()
    }
}




    pub async fn copy_in_raw_data(
        dsn: Dsn,
        table: String,
        columns: Vec<String>,
        where_clause: Option<Expr>,
        order_by: Option<Vec<Expr>>,
        limit: Option<u64>,
        offset: Option<u64>,
        data: Vec<BerolinaValue>,
    ) -> Result<()> {
        let postgres = Postgres::new(dsn, table, columns, where_clause, order_by, limit, offset);
        let causetq = CausetQ::new(dsn);
        let mut query = causetq.query(table);
        if let Some(where_clause) = &postgres.where_clause {
            query = query.where_clause(where_clause.clone());
        }
        if let Some(order_by) = &postgres.order_by {
            query = query.order_by(order_by.clone());
        }
        if let Some(limit) = &postgres.limit {
            query = query.limit(limit.clone());
        }
        if let Some(offset) = &postgres.offset {
            query = query.offset(offset.clone());
        }
        query.copy_in(data)

    }

    /// Issue a `COPY TO STDOUT` statement and transition the connection to streaming data
    /// from Postgres. This is a more efficient way to export data from Postgres but
    /// arrives in chunks of one of a few data formats (text/CSV/binary).
    ///
    /// If `statement` is anything other than a `COPY ... TO STDOUT ...` command,
    /// an error is returned.
    ///
    /// Note that once this process has begun, unless you read the stream to completion,
    /// it can only be canceled in two ways:
    ///
    /// 1. by closing the connection, or:
    /// 2. by using another connection to kill the server process that is sending the data as shown
    /// [in this StackOverflow answer](https://stackoverflow.com/a/35319598).
    ///
    /// If you don't read the stream to completion, the next time the connection is used it will
    /// need to read and discard all the remaining queued data, which could take some time.
    ///
    /// Command examples and accepted formats for `COPY` data are shown here:
    /// https://www.postgresql.org/docs/current/sql-copy.html
    #[allow(clippy::needless_lifetimes)]
    pub async fn copy_out_raw<'c>(
        statement: &str,
    ) -> Result<BoxStream<'c, Result<Bytes>>> {
        let mut conn = dsn.connect().await?;
        let mut stream = conn.copy_out(statement).await?;
        Ok(Box::pin(stream));

    }

    /// Issue a `COPY FROM STDIN` statement and transition the connection to streaming data
    /// to Postgres. This is a more efficient way to import data to Postgres but
    /// arrives in chunks of one of a few data formats (text/CSV/binary).
    /// If `statement` is anything other than a `COPY ... FROM STDIN ...` command,
    /// an error is returned.
    ///
    ///



    pub async fn copy_in_raw(
        statement: &str,
        data: BoxStream<Result<Bytes>>,
    ) -> Result<()> {
        let mut conn = dsn.connect().await?;
        let mut stream = conn.copy_in(statement).await?;
        for chunk in data.next().await {
            stream.write_all(chunk?).await?;
        }
        stream.finish().await?;
        Ok(())
    }

    /// Issue a `COPY FROM STDIN` statement and transition the connection to streaming data
    /// to Postgres. This is a more efficient way to import data to Postgres but
    /// arrives in chunks of one of a few data formats (text/CSV/binary).
    ///
    /// If `statement` is anything other than a `COPY ... FROM STDIN ...` command,
    /// an error is returned.





impl Pool<Postgres> {
    /// Issue a `COPY FROM STDIN` statement and begin streaming data to Postgres.
    /// This is a more efficient way to import data into Postgres as compared to
    /// `INSERT` but requires one of a few specific data formats (text/CSV/binary).
    ///
    /// A single connection will be checked out for the duration.
    ///
    /// If `statement` is anything other than a `COPY ... FROM STDIN ...` command, an error is
    /// returned.
    ///
    /// Command examples and accepted formats for `COPY` data are shown here:
    /// https://www.postgresql.org/docs/current/sql-copy.html
    ///
    /// ### Note
    /// [PgCopyIn::finish] or [PgCopyIn::abort] *must* be called when finished or the connection
    /// will return an error the next time it is used.
    pub async fn copy_in_raw(&self, statement: &str) -> Result<PgCopyIn<PoolConnection<Postgres>>> {
        PgCopyIn::begin(self.acquire().await?, statement).await
    }

    /// Issue a `COPY TO STDOUT` statement and begin streaming data
    /// from Postgres. This is a more efficient way to export data from Postgres but
    /// arrives in chunks of one of a few data formats (text/CSV/binary).
    ///
    /// If `statement` is anything other than a `COPY ... TO STDOUT ...` command,
    /// an error is returned.
    ///
    /// Note that once this process has begun, unless you read the stream to completion,
    /// it can only be canceled in two ways:
    ///
    /// 1. by closing the connection, or:
    /// 2. by using another connection to kill the server process that is sending the data as shown
    /// [in this StackOverflow answer](https://stackoverflow.com/a/35319598).
    ///
    /// If you don't read the stream to completion, the next time the connection is used it will
    /// need to read and discard all the remaining queued data, which could take some time.
    ///
    /// Command examples and accepted formats for `COPY` data are shown here:
    /// https://www.postgresql.org/docs/current/sql-copy.html
    pub async fn copy_out_raw(&self, statement: &str) -> Result<BoxStream<'static, Result<Bytes>>> {
        pg_begin_copy_out(self.acquire().await?, statement).await
    }
}

/// A connection in streaming `COPY FROM STDIN` mode.
///
/// Created by [PgConnection::copy_in_raw] or [Pool::copy_out_raw].
///
/// ### Note
/// [PgCopyIn::finish] or [PgCopyIn::abort] *must* be called when finished or the connection
/// will return an error the next time it is used.
#[must_use = "connection will error on next use if `.finish()` or `.abort()` is not called"]
pub struct PgCopyIn<C: DerefMut<Target = PgConnection>> {
    conn: Option<C>,
    response: CopyResponse,
}

impl<C: DerefMut<Target = PgConnection>> PgCopyIn<C> {
    async fn begin(mut conn: C, statement: &str) -> Result<Self> {
        conn.wait_until_ready().await?;
        conn.stream.send(Query(statement)).await?;

        let response: CopyResponse = conn
            .stream
            .recv_expect(MessageFormat::CopyInResponse)
            .await?;

        Ok(PgCopyIn {
            conn: Some(conn),
            response,
        })
    }

    /// Returns `true` if Postgres is expecting data in text or CSV format.
    pub fn is_textual(&self) -> bool {
        self.response.format == 0
    }

    /// Returns the number of columns expected in the input.
    pub fn num_columns(&self) -> usize {
        assert_eq!(
            self.response.num_columns as usize,
            self.response.format_codes.len(),
            "num_columns does not match format_codes.len()"
        );
        self.response.format_codes.len()
    }

    /// Check if a column is expecting data in text format (`true`) or binary format (`false`).
    ///
    /// ### Panics
    /// If `column` is out of range according to [`.num_columns()`][Self::num_columns].
    pub fn column_is_textual(&self, column: usize) -> bool {
        self.response.format_codes[column] == 0
    }

    /// Send a chunk of `COPY` data.
    ///
    /// If you're copying data from an `AsyncRead`, maybe consider [Self::read_from] instead.
    pub async fn send(&mut self, data: impl Deref<Target = [u8]>) -> Result<&mut Self> {
        self.conn
            .as_deref_mut()
            .expect("send_data: conn taken")
            .stream
            .send(CopyData(data))
            .await?;

        Ok(self)
    }

    //Now we conjoin the index of the column with the data.
    pub async fn send_with_index(&mut self, data: impl Deref<Target = [u8]>, index: usize) -> Result<&mut Self> {
        self.conn
            .as_deref_mut()
            .expect("send_data: conn taken")
            .stream
            .send(CopyDataWithIndex(data, index))
            .await?;

        Ok(self)
    }

    /// Send a chunk of `COPY` data from an `AsyncRead`.
    /// This is a convenience method that wraps [`AsyncRead::read_to_end`] and [`PgCopyIn::send`].
    /// It's a bit more efficient than [`PgCopyIn::send`] because it avoids a buffer.
    /// It's also a bit more complicated to use, but it's easier to understand.
    /// See the [PgCopyIn::send] docs for more information.
    ///   Unless required by applicable law or agreed to in writing, software
    ///  distributed under the License is distributed on an "AS IS" BASIS,
    /// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    /// See the License for the specific language governing permissions and
    /// limitations under the License.
    /// =================================================================
    ///


    pub async fn read_from<R: AsyncRead + Unpin>(&mut self, reader: R) -> Result<&mut Self> {
        let mut buf = [0u8; 8192];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            self.send(&buf[..n]).await?;
        }
        Ok(self)
    }

    /// Abort the `COPY` operation.
    /// This will cause the `COPY` operation to return an error.
    /// This is useful if you want to stop the `COPY` operation early.

    pub async fn abort(&mut self) -> Result<&mut Self> {
        self.conn
            .as_deref_mut()
            .expect("abort: conn taken")
            .stream
            .send(CopyFail)
            .await?;
        Ok(self)
    }

    /// Finish the `COPY` operation.
    /// This will cause the `COPY` operation to return an error.
    /// This is useful if you want to stop the `COPY` operation early.

    pub async fn finish(&mut self) -> Result<&mut Self> {
        self.conn
            .as_deref_mut()
            .expect("finish: conn taken")
            .stream
            .send(CopyDone)
            .await?;
        Ok(self)
    }
    }

    /// Abort the `COPY` operation.
    /// This will cause the server to return an error the next time the connection is used.
    /// This is the same as calling [PgCopyIn::finish] followed by [PgCopyIn::abort].
    /// This is the recommended way to cancel a `COPY` operation.
    /// See [PgCopyIn::finish] for more information.
    /// ### Note

    /// This is the recommended way to cancel a `COPY` operation.
    /// See [PgCopyIn::finish] for more information.
    ///








#[cfg(test)]
mod pgtests {
        use supertest::*;
        use supertest::test::TestRequest;
        use supertest::test::TestResponse;

        use crate::{PgConnection, PgPool};
        use crate::types::{PgType, PgTypeInfo};

        use std::io::Cursor;
        use std::io::Read;

        use futures::StreamExt;

        use tokio::runtime::Runtime;


        #[tokio::test]
        async fn test_copy_in() {
            let mut runtime = Runtime::new().unwrap();
            let pool = PgPool::new(PgConnection::connect("postgres://postgres@localhost:5432").await.unwrap());
            let conn = pool.get_connection().await.unwrap();
            let mut copy_in = PgCopyIn::begin(conn, "COPY test_copy_in FROM STDIN").await.unwrap();
            let mut buf = Cursor::new(b"1\n2\n3\n");
            copy_in.read_from(buf).await.unwrap();
            copy_in.finish().await.unwrap();
            let mut copy_in = PgCopyIn::begin(conn, "COPY test_copy_in FROM STDIN").await.unwrap();
            let mut buf = Cursor::new(b"4\n5\n6\n");
            copy_in.read_from(buf).await.unwrap();
            copy_in.finish().await.unwrap();
            let mut copy_in = PgCopyIn::begin(conn, "COPY test_copy_in FROM STDIN").await.unwrap();
            let mut buf = Cursor::new(b"7\n8\n9\n");
            copy_in.read_from(buf).await.unwrap();
            copy_in.finish().await.unwrap();
            let mut copy_in = PgCopyIn::begin(conn, "COPY test_copy_in FROM STDIN").await.unwrap();
            let mut buf = Cursor::new(b"10\n11\n12\n");
            copy_in.read_from(buf).await.unwrap();
            copy_in.finish().await.unwrap();
            let mut copy_in = PgCopyIn::begin(conn, "COPY test_copy_in FROM STDIN").await.unwrap();
            let mut buf = Cursor::new(b"13\n14\n15\new_range\n");
            copy_in.read_from(buf).await.unwrap();
            copy_in.finish().await.unwrap();
        }
    }



    #[cfg(test)]
    mod testspg1 {
        pub enum TestError {
            TestError,
        }

        impl std::fmt::Display for TestError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "TestError")
            }
        }
    }


pub fn main() {
    let mut runtime = Runtime::new().unwrap();
    let pool = PgPool::new("postgres://postgres:postgres@localhost:5432").unwrap();
    let conn = pool.get().unwrap();
    let mut copy_in = PgCopyIn::new(&conn, "COPY test_copy_in (a, b) FROM STDIN").unwrap();
    let mut buf = Cursor::new(b"1,2\n3,4\n");
    runtime.block_on(copy_in.read_from(&mut buf)).unwrap();
    copy_in.finish().unwrap();
}


#[test]
fn test_copy_in() {
    let mut runtime = Runtime::new().unwrap();
    let pool = PgPool::new("postgres://postgres:postgres@localhost:5432").unwrap();
    let conn = pool.get().unwrap();
    let mut copy_in = PgCopyIn::new(&conn, "COPY test_copy_in (a, b) FROM STDIN").unwrap();
    let mut buf = Cursor::new(b"1,2\n3,4\n");
    runtime.block_on(copy_in.read_from(&mut buf)).unwrap();
    copy_in.finish().unwrap();
}


#[test]
fn test_copy_in_with_index() {
    let conn: &mut PgConnection = &mut PgConnection::connect("postgres://postgres:postgres@localhost:5432").unwrap();
    let mut copy_in = PgCopyIn::new(conn, "COPY test_copy_in (a, b) FROM STDIN").unwrap();
    let mut buf = Cursor::new(b"1,2\n3,4\n");
    copy_in.read_from(&mut buf).unwrap();


    let mut buf = [0u8; 8192];
    loop {
        let n = copy_in.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        conn.stream.send(CopyData(buf[..n])).await?;
    }


    conn.stream.send(CopyDone).await?;

    // flush any existing messages in the buffer and clear it
    conn.stream.flush().await?;

    {
        let buf_stream = &mut *conn.stream;
        let stream = &mut buf_stream.stream;

        // ensures the buffer isn't left in an inconsistent state
        let mut guard = BufGuard(&mut buf_stream.wbuf);

        let buf: &mut Vec<u8> = &mut guard.0;
        buf.push(b'd'); // CopyData format code
        buf.resize(5, 0); // reserve space for the length
    }
    conn.stream.flush().await?;
}


#[test]
fn test_copy_in_with_index_2() {
            loop {
                let read = match () {
                    // Tokio lets us read into the buffer without zeroing first
                    #[cfg(any(feature = "runtime-tokio", feature = "runtime-actix"))]
                    _ if buf.len() != buf.capacity() => {
                        // in case we have some data in the buffer, which can occur
                        // if the previous write did not fill the buffer
                        buf.truncate(5);
                        source.read_buf(buf).await?
                    }
                    _ => {
                        // should be a no-op unless len != capacity
                        buf.resize(buf.capacity(), 0);
                        source.read(&mut buf[5..]).await?
                    }
                };

                if read == 0 {
                    break;
                }

                let read32 = u32::try_from(read)
                    .map_err(|_| err_protocol!("number of bytes read exceeds 2^32: {}", read))?;
                buf[0] = b'd'; // CopyData format code
                buf[1] = (read32 >> 24) as u8;

                (&mut buf[1..]).put_u32(read32 + 4);

                conn.stream.send(CopyData(buf[..read + 5])).await?;
            }
                stream.write_all(&buf[..read + 5]).await?;
                stream.flush().await?;


            conn.stream.send(CopyDone).await?;
            stream.write_all(&[b'\0', b'\0', b'\0', b'd']).await?;
            stream.flush().await?;

            // flush any existing messages in the buffer and clear it
            conn.stream.flush().await?;

    {
                    let buf_stream = &mut *conn.stream;
                    let stream = &mut buf_stream.stream;

                    // ensures the buffer isn't left in an inconsistent state
                    let mut guard = BufGuard(&mut buf_stream.wbuf);

                    let buf: &mut Vec<u8> = &mut guard.0;
                    buf.push(b'd'); // CopyData format code
                    buf.resize(5, 0); // reserve space for the length
                }
                conn.stream.flush().await?;   // flush any existing messages in the buffer and clear it
                stream.flush().await?;
}





#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyInState {
    Ready,
    InProgress,
    Finished,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyOutState {


    Ready,
    InProgress,
    Finished,
}


fn copy_in_state() -> CopyInState {
    msg.as_bytes().to_vec();
    CopyInState::abort(msg)
}











async fn pg_begin_copy_out<'c, C: DerefMut<Target = PgConnection> + Send + 'c>(
    mut conn: C,
    statement: &str,
) -> Result<BoxStream<'c, Result<Bytes>>> {
    conn.wait_until_ready().await?;
    conn.stream.send(Query(statement)).await?;

    let _: CopyResponse = conn
        .stream
        .recv_expect(MessageFormat::CopyOutResponse)
        .await?;

    let stream: TryAsyncStream<'c, Bytes> = try_stream! {
        loop {
            let msg = conn.stream.recv().await?;
            match msg.format {
                MessageFormat::CopyData => r#yield!(msg.decode::<CopyData<Bytes>>()?.0),
                MessageFormat::CopyDone => {
                    let _ = msg.decode::<CopyDone>()?;
                    conn.stream.recv_expect(MessageFormat::CommandComplete).await?;
                    conn.stream.recv_expect(MessageFormat::ReadyForQuery).await?;
                    return Ok(())
                },
                _ => return Err(err_protocol!("unexpected message format during copy out: {:?}", msg.format))
            }
        }
    };

    Ok(Box::pin(stream))
}



#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeBound<T> {

    Inclusive(T),
    Exclusive(T),
}




#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Range<T> {
    pub start: RangeBound<T>,
    pub end: RangeBound<T>,
}



impl<T: PartialOrd> RangeIter<T> {
    pub fn new(ranges: Vec<Range<T>>) -> Self {
        Self {
            ranges,
            current_range: 0,
            current_range_index: 0,
            current: 0,

        }
    }
}




impl<T: PartialOrd> Iterator for RangeIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_range_index >= self.ranges.len() {
            return None;
        }
        let mut range = &self.ranges[self.current_range_index];
        if self.current_range == 0 {
            self.current = range.start;
        }
        if self.current >= range.end {
            self.current_range_index += 1;
            if self.current_range_index >= self.ranges.len() {
                return None;
            }
            range = &self.ranges[self.current_range_index];
            self.current = range.start;
        }
        self.current_range += 1;
        Some(self.current)
    }
}



impl<T: PartialOrd> Iterator for RangeIter<T> {

    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.ranges.len() {
            return None;
        }
        let mut current = &self.ranges[self.current];
        let mut value = current.start;
        if value > current.end {
            self.current += 1;
            return self.next();
        }
        if current.start == current.end {
            self.current += 1;
            return self.next();
        }
        if current.start == value {
            value = current.end;
            if current.end == value {
                self.current += 1;
                return self.next();
            }
        }
        self.current += 1;
        Some(value)
    }
}


impl<T: PartialOrd> Debug for RangeIter<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RangeIter {{ ranges: {:?} }}", self.ranges)
    }
}


impl<T: PartialOrd> Display for RangeIter<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RangeIter {{ ranges: {:?} }}", self.ranges)
    }
}


impl<T: PartialOrd> PartialEq for RangeIter<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ranges == other.ranges
    }
}


impl<T: PartialOrd> Eq for RangeIter<T> {}



#[derive(PartialEq, Eq, Clone, Debug)]
pub enum IterStatus {
    /// The iterator is exhausted.
    Done,
    /// The iterator is not exhausted.
    /// The iterator is exhausted if the next element is greater than or equal to `upper`.
    /// The iterator is exhausted if the next element is less than `lower`.
    ///
    /// All ranges are consumed.
    Drained,

    /// The iterator is not exhausted.
    /// Last range is drained or this iteration is a fresh start so that caller should mutant_search
    /// on a new range.
    New(),

    /// Last interval range is not drained and the caller should continue mutant_searchning without changing
    /// the mutant_search range.
    Continue,


}

/// An iterator like structure that produces user soliton_id ranges.
///
/// For each `next()`, it produces one of the following:
/// - a new range
/// - a flag indicating continuing last interval range
/// - a flag indicating that all ranges are consumed
///
/// If a new range is returned, caller can then mutant_search unCausetLocaleNucleon amount of soliton_id(s) within this new range.
/// The caller must inform the structure so that it will emit a new range next time by calling
/// `notify_drained()` after current range is drained. Multiple `notify_drained()` without `next()`
/// will have no effect.
pub struct Iteron {
    ranges: Vec<Range<u64>>,

    current_range: usize,

    current_range_index: usize,

    current: u64,

    status: IterStatus,

    last_range_drained: bool,

    last_range_start: u64,

    last_range_end: u64,

    /// Whether or not we are processing a valid range. If we are not processing a range, or there
    /// is no range any more, this field is `false`.
    in_range: bool,

    /// The current range.
    /// If `in_range` is `false`, this field is ignored.
    ///
    /// If `in_range` is `true`, this field is the current range.
    ///
    /// If `in_range` is `true` and there is no range any more, this field is `None`.
    ///
    /// If `in_range` is `true` and there is a range, this field is `Some(range)`.


    range: Option<Range>,

    iter: std::vec::IntoIter<>,

    /// The current soliton_id.

    soliton_id: SolitonId,

    /// The current soliton_id range.
    /// If `in_range` is `false`, this field is ignored.
    /// If `in_range` is `true`, this field is the current soliton_id range.
    /// If `in_range` is `true` and there is no range any more, this field is `None`.
    /// If `in_range` is `true` and there is a range, this field is `Some(range)`.
    /// If `in_range` is `true` and there is a range, this field is `Some(range)`.
    ///
    ///
    soliton_id_range: Option<Range>,

    /// The current soliton_id.
    /// If `in_range` is `false`, this field is ignored.
    /// If `in_range` is `true`, this field is the current soliton_id.
    /// If `in_range` is `true` and there is no range any more, this field is `None`.
    ///
}


impl Iteron {







    /// Creates a new iterator that produces ranges.
    ///
    /// The iterator will produce ranges in the following order:
    /// - The range [0, `lower`)
    /// - The range [`lower`, `upper`)
    /// - The range [`upper`, `upper` + 1)
    /// - The range [`upper` + 1, `upper` + 1 + `lower`)
    /// - The range [`upper` + 1 + `lower`, `upper` + 1 + `lower` + `upper`)
    /// - The range [`upper` + 1 + `lower` + `upper`, `upper` + 1 + `lower` + `upper` + 1)
    /// - ...
    /// - The range [`upper` + 1 + `lower` + `upper` + 1 + `upper` + 1, `upper` + 1 + `lower` + `upper` + 1 + `upper` + 1 + `lower` + 1)
    ///
    ///
    /// `lower` and `upper` must be non-negative.
    ///
    /// # Panics
    ///
    /// Panics if `lower` is greater than `upper`.
    pub fn new(lower: SolitonId, upper: SolitonId) -> Self {
        assert!(lower <= upper, "lower should be less than or equal to upper");
        let mut ranges = Vec::new();
        let mut current = 0;
        let mut current_range = 0;

        let mut iter = Vec::new();
        let mut soliton_id = 0;
        while soliton_id < upper {
            iter.push(Range::new(soliton_id, min(soliton_id + lower, upper)));
            soliton_id += lower + 1;
        }
        iter.push(Range::new(soliton_id, upper + 1));
        iter.push(Range::new(upper + 1, upper + 1 + lower));
        iter.push(Range::new(upper + 1 + lower, upper + 1 + lower + upper));
    }

    /// Returns the current soliton_id range.
    ///
    /// If the iterator is exhausted, `None` is returned.
    pub fn soliton_id_range(&self) -> Option<Range> {
        self.soliton_id_range
    }

    /// Returns the current range.
    /// If the iterator is exhausted, `None` is returned.
    ///

    pub fn range(&self) -> Option<Range> {
        self.range
    }
}

impl sIterator {
    #[inline]
    pub fn new(user_soliton_id_ranges: Vec<>) -> Self {
        Self {
            in_range: false,
            range: (),
            iter: user_soliton_id_ranges.into_iter(),
            soliton_id: (),
            soliton_id_range: ()
        }
    }

    /// Continues iterating.
    #[inline]
    pub fn next(&mut self) -> IterStatus {
        if self.in_range {
            return IterStatus::Continue;
        }
        if let Some(range) = self.iter.next() {
            self.in_range = true;
            self.range = Some(range);
            self.soliton_id = range.lower;
            self.soliton_id_range = Some(range);
            return IterStatus::Continue;
        }
        IterStatus::End
    }

    /// Returns the current soliton_id.
    /// If the iterator is exhausted, `None` is returned.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not in range.
    ///
    /// # Examples
    ///
    /// ```
    /// use soliton_id::{SolitonId, RangesIterator};
    ///
    /// let mut iter = RangesIterator::new(vec![Range::new(0, 10)]);
    /// assert_eq!(iter.soliton_id(), Some(0));
    /// iter.next();
    /// assert_eq!(iter.soliton_id(), Some(1));
    /// iter.next();
    /// assert_eq!(iter.soliton_id(), Some(2));
    /// iter.next();
    /// assert_eq!(iter.soliton_id(), Some(3));
    /// iter.next();


    pub fn soliton_id(&self) -> Option<SolitonId> {
        match self.iter.next() {
            _None => IterStatus::Drained,
            Some(range) => {
                self.in_range = true;
                self.range = Some(range);
                self.soliton_id = range.lower;
                self.soliton_id_range = Some(range);
                Some(self.soliton_id)
            }

            Some(range) => {

                self.in_range = true;
                self.range = Some(range);
                self.soliton_id = range.lower;
                self.soliton_id_range = Some(range);
                Some(self.soliton_id)

            }


        }



    }


    /// Returns the current soliton_id range.
    /// If the iterator is exhausted, `None` is returned.
    /// # Panics
    /// Panics if the iterator is not in range.
    /// # Examples
    /// ```
    /// use soliton_id::{SolitonId, RangesIterator};
    /// let mut iter = RangesIterator::new(vec![Range::new(0, 10)]);
    /// assert_eq!(iter.soliton_id_range(), Some(Range::new(0, 10)));
    /// iter.next();
    /// assert_eq!(iter.soliton_id_range(), Some(Range::new(10, 20)));
    /// iter.next();
    /// assert_eq!(iter.soliton_id_range(), Some(Range::new(20, 30)));
    /// iter.next();
    ///
    /// ```
    /// # Panics
    /// Panics if the iterator is not in range.
    /// # Examples
    /// ```
    /// use soliton_id::{SolitonId, RangesIterator};
    /// let mut iter = RangesIterator::new(vec![Range::new(0, 10)]);




    /// Notifies that current range is drained.
    #[inline]
    pub fn notify_drained(&mut self) {
        self.in_range = false;
    }
}
