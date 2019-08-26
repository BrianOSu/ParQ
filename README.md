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

## Issues

### Mixed Lists

Mixed lists can lead to unexpected behaviour. By default it will try to write them as binary column. This is due to the fact that parquet files don't have a mixed column type.

### Writing Row Groups

Concurrently calling .pq.writeMulti will keep appending row groups containing your data to the table. When you are finished writing, call .pq.closeWriter to finalise the footer for your parquet file.

