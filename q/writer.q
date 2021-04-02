//////////////////////////////////////////////////////////////////////////////
//   Copyright 2020 Brian O'Sullivan
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
// Functions to write parquet files:
//   * Compression
//   * Single row group writing
//   * Multi row group writing
//////////////////////////////////////////////////////////////////////////////


//////////////////////////////////////////////////////////////////////////////
// Compression codecs
//////////////////////////////////////////////////////////////////////////////

///
// Define the available codecs
// https://github.com/apache/arrow/blob/master/cpp/src/arrow/util/type_fwd.h#L42
// Parquet doesn't neccessarily support all of these
.pq.write.codecs:(`UNCOMPRESSED`SNAPPY`GZIP`BROTLI`ZSTD`LZ4`LZ4_FRAME`LZO`BZ2`LZ4_HADOOP)!til 10

///
// Sets the compression codec for writing files
// @param  Codec - Sym from .pq.codecs
.pq.write.setCodec:{[codec]
    if[not -11h~type codec;
        '"Codec must be a sym"];
    if[not codec in key .pq.write.codecs;
        '"Codec must exist in .pq.codecs"];
    .pq.priv.codec:.pq.write.codecs[codec];
 }

///
// Set defaults codec to ZSTD
.pq.write.setCodec[`UNCOMPRESSED]

///
// Returns the compression the codec is set to
// @return Sym - The default codec to write with
.pq.write.getCodec:{.pq.write.codecs?.pq.priv.codec}


//////////////////////////////////////////////////////////////////////////////
// Write multiple rows groups to a parquet file
//////////////////////////////////////////////////////////////////////////////

///
// Write a table to a parquet file
// @param  Table      - Table to write, use select from t for splayed tables
// @param  FilePath   - Filepath as a Sym/string, doesn't support hysm
// @param  Single     - Write a single rowgroup
//                      Otherwise keep file handle open for future row groups
// @param  Codec      - Codec to compress the file with, see .pq.codecs
// @param  KVMetadata - Dictionary of strings/symbols to write key value meta data
// @return Bool       - 1b if writes, otherwise throws error
.pq.priv.write:.pq.priv.libPath 2:(`writer;5)

///
// Write a table to a parquet file
// Defaults to multi row group, and doesn't append to existing
// @param  Table    - Table to write
// @param  FilePath - Filepath as a Sym/string, doesn't support hysm
// @return Bool     - 1b if writes, otherwise throws error
.pq.write.multi:{[t;f]
    .pq.priv.write[select from t;f;0b;.pq.priv.codec;(::)]
 }

 ///
// Write a table to a parquet file
// Defaults to multi row group, and doesn't append to existing
// @param  Table      - Table to write
// @param  FilePath   - Filepath as a Sym/string, doesn't support hysm
// @param  KVMetadata - Dictionary of strings/symbols to write key value meta data
// @return Bool       - 1b if writes, otherwise throws error
.pq.write.multiMeta:{[t;f;m]
    .pq.priv.write[select from t;f;0b;.pq.priv.codec;m]
 }

///
// Close the loaded parquet file
// @return Bool - 1b if closes, otherwise throws error
.pq.write.close:.pq.priv.libPath 2:(`closeW;1) 


//////////////////////////////////////////////////////////////////////////////
// Write a single row group to a parquet file. 
//////////////////////////////////////////////////////////////////////////////

///
// Write a table to a parquet file as a single row group.
// Does not append to existing. Automatically closes file.
// @param  Table    - Table to write
// @param  FilePath - Filepath as a Sym/string, doesn't support hysm
// @return Bool     - 1b if writes, otherwise throws error
.pq.write.single:{[t;f]
    .pq.priv.write[select from t;f;1b;.pq.priv.codec;(::)]
 }

///
// Write a table to a parquet file without key value metadata
// @param  Table    - Table to write, use select from t for splayed tables
// @param  FilePath - Filepath as a Sym/string, doesn't support hysm
// @param  Single   - Write a single rowgroup
//                    Otherwise keep file handle open for future row groups
// @param  KVMetadata - Dictionary of strings or symbols to write key value meta data
// @return Bool     - 1b if writes, otherwise throws error
.pq.write.singleMeta:{[t;f;m]
    .pq.priv.write[select from t;f;1b;.pq.priv.codec;m]
 }