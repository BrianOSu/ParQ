# ParQ

![alt text](https://github.com/BrianOSu/ParQ/blob/master/parq_logo.jpg?raw=true)

KDB library to read and write parquet files.

Author: Brian O'Sullivan

Email: b.osullivan@live.ie

LinkedIn: https://www.linkedin.com/in/brian-o-sullivan-b98b68a4/

## Getting Started

### Prerequisites

- kdb+
- Apache Arrow
  * https://github.com/apache/arrow
  * Built for ver 1.0.0
  * Older versions codec dictionary will be incorrect

Currently only supports linux

### Type Mapping

*Kdb -> Parquet:*

| KdbType   | ParquetType          | LogicalType       |
|-----------|----------------------|-------------------|
| boolean   | BOOLEAN              | None              |
| guid      | FIXED_LEN_BYTE_ARRAY | UUID              |
| byte      | FIXED_LEN_BYTE_ARRAY | None(size 1)      |
| short     | INT32                | (16, signed)      |
| int       | INT32                | (32, signed)      |
| long      | INT64                | (64, signed)      |    
| real      | FLOAT                | None              |
| float     | DOUBLE               | None              |     
| char      | BYTE_ARRAY           | String            |
| string    | BYTE_ARRAY           | String            |
| symbol    | BYTE_ARRAY           | Enum              |
| timestamp | INT64                | Timestamp(Nanos)  |
| month     | INT32                | None              |
| date      | INT32                | Date              |
| datetime  | DOUBLE               | None              |
| timespan  | INT64                | Time(Nanos)       |
| minute    | INT32                | None              |
| second    | INT32                | None              |
| time      | INT32                | Time(Millis)      |
| *         | BYTE_ARRAY           | None              |

*Parquet -> Kdb:*

| ParquetType          | LogicalType      | KdbType   |
|----------------------|------------------|-----------|
| BOOLEAN              | None             | boolean   |
| INT32                | None             | int       |
| INT32                | (16, signed)     | short     |
| INT32                | TIME             | time      |
| INT32                | DATE             | date      |
| INT64                | None             | long      |
| INT64                | TIMESTAMP(Nanos) | timestamp |
| INT64                | TIME(NANOS)      | timespan  |
| INT96                | None             | timestamp |
| FLOAT                | None             | real      |
| DOUBLE               | None             | float     |
| BYTE_ARRAY           | String           | string    |
| BYTE_ARRAY           | Enum             | sym       |
| BYTE_ARRAY           | None             | byte list |
| FIXED_LEN_BYTE_ARRAY | UUID             | guid      |
| FIXED_LEN_BYTE_ARRAY | Byte length = 1  | byte      |
| FIXED_LEN_BYTE_ARRAY | None             | byte list |

If the logical type is not in the above list, ParQ will default to treating it as None.

### Usage instructions

Start q with ParQ
```bash
$ q q/ParQ.q
```
Writing and reading can be done as in the following:
```q
q)t:([]a:1 2 3;b:("testing";"this";"here"))
q).pq.write.single[t;`t]
1b
q)//Read a single row group from a parquet file
q)t1:.pq.read.first`t
q)t~t1
1b
```
Multiple Row groups 
```q
q)t:([]a:1 2 3;b:("testing";"this";"here"))
q)//Write the first row group
q).pq.write.multi[t;`t]
1b
//Write the second row group
q).pq.write.multi[t;`t]
1b
q)//Close the writer to finalise the file
q).pq.write.close[]
1b
q)//Open the file to read multiple row groups
q).pq.read.load`t
1b
q).pq.read.schema[]
"message schema {"
"  required int64 a (Int(bitWidth=64, isSigned=true));"
"  required binary b (String);"
,"}"
//Current rowGroup
q).pq.read.rowGroup[]
0
//Total row groups
q).pq.read.totalRowGroup[]
2
//Read the first row group
q)t2:.pq.read.multi[]
q)t~t2
1b
//Move to the next row group
q).pq.read.next[]
1b
//Read the current row group
q)t2,:.pq.read.multi[]
q)t2~t
0b
q)t2~t,t
1b
q).pq.read.next[]
'Already at the latest row group
  [0]  .pq.next[]
       ^
//Close file when finished reading
q).pq.read.close[]
1b
```
Multi-threaded rowgroup reading using peach with slaves
```q
q).pq.read.load[`t.parquet]
1b
q)t:raze .pq.read.group[`t.parquet;;(::)] peach til .pq.read.totalRowGroup[]
q).pq.read.close[]
1b
```
It is much quicker to read specific columns into memory if you don't need the full table.
```q
//Load the table
q).pq.read.load`t.parquet
1b
//Check the schema
q).pq.read.schema[]
"message schema {"
"  required boolean bool;"
"  required fixed_len_byte_array(16) guid (UUID);"
"  required fixed_len_byte_array(1) byte;"
"  required int32 short (Int(bitWidth=16, isSigned=true));"
"  required int32 int (Int(bitWidth=32, isSigned=true));"
"  required int64 long (Int(bitWidth=64, isSigned=true));"
"  required float real;"
"  required double float;"
"  required binary char (String);"
"  required binary syms (Enum);"
"  required binary strings (String);"
"  required int64 timestamp (Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));"
"  required int32 month;"
"  required int32 date (Date);"
"  required double datetime;"
"  required int64 timespan (Time(isAdjustedToUTC=false, timeUnit=nanoseconds));"
"  required int32 minute;"
"  required int32 second;"
"  required int32 time (Time(isAdjustedToUTC=false, timeUnit=milliseconds));"
"}"
//Read only the float and int columns
q).pq.read.multi[`float`int]
float    int       
-------------------
57.68374 973227945 
23.94061 2131280630
52.43847 989873294 
..
//Close the file
q).pq.read.close[]
1b
//Read guid, bool and int column from first row group
q).pq.read.group[`t.parquet; 0; `guid`bool`int]
guid                                 bool int       
----------------------------------------------------
a0d3e6c6-9bfa-3936-8fec-94bd844b4a20 1    963573298 
39f24b49-8d5c-fa51-97bc-e2689b52f052 0    467447548 
3c669a3f-7071-f3e4-142f-8bbb684d299d 0    1302452830
..
//Read int,bool and time column from first row group
q).pq.read.group[`t.parquet; 0; `int`bool`time]
int        bool time        
----------------------------
973227945  1    00:00:05.480
2131280630 1    00:07:54.634
989873294  0    00:07:32.442
..
```
### Key Value Metadata

ParQ allows users to write their own key-value metadata. This can used to indicate which datatype the column originated in.

Available functions are:
  * .pq.write.singleMeta
  * .pq.write.multiMeta
  * .pq.read.keyValueMeta

```q
q)t:([]date:10?.z.d; price:10?100)
q).pq.write.singleMeta[t; "t.parquet"; (`date`price)!(`date`long)]
1b
q).pq.read.load"t.parquet"
1b
q).pq.read.keyValueMeta[]
date | date
price| long
```

### Compression

[All parquet compression codecs are supported. ](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L32)

Compression can be set using the following:
```q
q).pq.write.codecs
UNCOMPRESSED| 0
SNAPPY      | 1
GZIP        | 2
BROTLI      | 3
ZSTD        | 4
LZ4         | 5
LZO         | 6
BZ2         | 7
q).pq.write.setCodec[`ZSTD]
q).pq.write.getCodec[]
`ZSTD
```

By default, compression is set to ZSTD. From testing, its results have been extremely promissing.

## Issues

### Mixed Lists

Mixed lists can lead to unexpected behaviour. By default it will try to write them as binary column. This is due to the fact that parquet files don't have a mixed column type.

### Writing Row Groups

Concurrently calling .pq.writeMulti will keep appending row groups containing your data to the table. When you are finished writing, call .pq.closeWriter to finalise the footer for your parquet file.

