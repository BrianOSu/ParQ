# ParQ

KDB library to read and write parquet files.

Author: Brian O'Sullivan

Email: b.osullivan@live.ie

LinkedIn: https://www.linkedin.com/in/brian-o-sullivan-b98b68a4/

## Getting Started

### Prerequisites

- kdb+
- Apache Arrow
  * https://github.com/apache/arrow
  * Built for ver 0.16.0
  * 0.14.0 codec dictionary will be incorrect

Currently only supports linux

### Usage instructions

Start q with ParQ
```bash
$ q q/ParQ.q
```
Writing and reading can be done as in the following:
```q
q)t:([]a:1 2 3;b:("testing";"this";"here"))
q).pq.write[t;`t]
1b
q)//Read a single row group from a parquet file
q)t1:.pq.read`t
q)t~t1
1b
```
Multiple Row groups 
```q
q)t:([]a:1 2 3;b:("testing";"this";"here"))
q)//Write the first row group
q).pq.writeMulti[t;`t]
1b
//Write the second row group
q).pq.writeMulti[t;`t]
1b
q)//Close the writer to finalise the file
q).pq.closeWriter[]
1b
q)//Open the file to read multiple row groups
q).pq.load`t
1b
q).pq.schema[]
"message schema {"
"  required int64 a (Int(bitWidth=64, isSigned=true));"
"  required binary b (String);"
,"}"
//Current rowGroup
q).pq.rowGroup[]
0
//Total row groups
q).pq.totalRowGroup[]
2
//Read the first row group
q)t2:.pq.readMulti[]
q)t~t2
1b
//Move to the next row group
q).pq.next[]
1b
//Read the current row group
q)t2,:.pq.readMulti[]
q)t2~t
0b
q)t2~t,t
1b
q).pq.next[]
'Already at the latest row group
  [0]  .pq.next[]
       ^
//Close file when finished reading
q).pq.close[]
1b
```
It is much quicker to read specific columns into memory if you don't need the full table.
```q
//Load the table
q).pq.load`t.parquet
1b
//Check the schema
q).pq.schema[]
"message schema {"
"  required boolean bool;"
"  required fixed_len_byte_array(16) guid;"
"  required fixed_len_byte_array(1) byte;"
"  required int32 short (Int(bitWidth=16, isSigned=true));"
"  required int32 int;"
"  required int64 long (Int(bitWidth=64, isSigned=true));"
"  required float real;"
"  required double float;"
"  required binary char (String);"
"  required binary syms (String);"
"  required binary strings (String);"
"  required int96 timestamp;"
"  required int32 month;"
"  required int32 date (Date);"
"  required double datetime;"
"  required int96 timespan;"
"  required int32 minute;"
"  required int32 second;"
"  required int32 time (Time(isAdjustedToUTC=true, timeUnit=milliseconds));"
//Read only the float and int columns
q).pq.readMulti[`float`int]
float    int       
-------------------
57.68374 973227945 
23.94061 2131280630
52.43847 989873294 
..
//Close the file
q).pq.close[]
1b
//Read guid, bool and int column from first row group
q).pq.readGroup[`t.parquet; 0; `guid`bool`int]
guid                               bool int       
--------------------------------------------------
0x5898462e1abb80d7f7bd9a7d931aba9d 1    973227945 
0x1db424985c3ebdff38d6578aa052f23d 1    2131280630
0xd6cc4cd6919d1dc8921bc7dce7b481ee 0    989873294 
..
//Read int,bool and time column from first row group
q).pq.readGroup[`t.parquet; 0; `int`bool`time]
int        bool time        
----------------------------
973227945  1    00:00:05.480
2131280630 1    00:07:54.634
989873294  0    00:07:32.442
..
```
### Compression

[All parquet compression codecs are supported. ](https://github.com/apache/parquet-format/blob/54e6133e887a6ea90501ddd72fff5312b7038a7c/src/main/thrift/parquet.thrift#L461)

Compression can be set using the following:
```q
q).pq.codecs
UNCOMPRESSED| 0
SNAPPY      | 1
GZIP        | 2
LZO         | 3
BROTLI      | 4
LZ4         | 5
ZSTD        | 6
q).pq.setCodec[`ZSTD]
q).pq.getCodec[]
`UNCOMPRZSTDESSED
```

By default, compression is set to ZSTD. From testing, its results have been extremely promissing.

## Issues

### Mixed Lists

Mixed lists can lead to unexpected behaviour. By default it will try to write them as binary column. This is due to the fact that parquet files don't have a mixed column type.

### Writing Row Groups

Concurrently calling .pq.writeMulti will keep appending row groups containing your data to the table. When you are finished writing, call .pq.closeWriter to finalise the footer for your parquet file.

