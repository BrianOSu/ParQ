/////////////////////////////////////////////////////
// ParQ API to read/write parquet files from within q
// Author: Brian O'Sullivan
// Email: b.osullivan@live.ie
/////////////////////////////////////////////////////

///
// Set the path to the .so file
.pq.priv.libPath:`$"install/ParQ"

///
// Opens and reads a given row group in the supplied parquet file.
// @param file string/sym
// @param group long
// @param cols list of syms representing cols to read, or (::) for all
.pq.readGroup:.pq.priv.libPath 2:(`readGroup;3) 

///
// Opens and reads the first row group in the supplied parquet file.
// @param file string/sym
.pq.read:.pq.readGroup[;0j;(::)]

///
// Load the parquet file to make it ready for reading
// @param file string/sym
.pq.load:.pq.priv.libPath 2:(`initReader;1) 

///
// Read the current row group for loaded file
// @param cols list of syms representing cols to read, or no cols for all
// e.g .pq.readMulti returns all cols
.pq.readMulti:.pq.priv.libPath 2:(`readMulti;1) 

///
// Move to next row group for reading
.pq.next:.pq.priv.libPath 2:(`nextRowGroup;1) 

///
// Close the loaded parquet file
.pq.close:.pq.priv.libPath 2:(`closeP;1) 

///
// The row group being pointed at for loaded file
.pq.rowGroup:.pq.priv.libPath 2:(`currentRowGroup;1) 

///
// The total number of row groups in the loaded file
.pq.totalRowGroup:.pq.priv.libPath 2:(`totalRowGroup;1) 

///
// Displa the parquet schema as string for loaded file
.pq.priv.schema:.pq.priv.libPath 2:(`readSchema;1)
.pq.schema:{[] -1_"\n" vs .pq.priv.schema[]}

///
// Define the available codecs
.pq.codecs:(`UNCOMPRESSED`SNAPPY`GZIP`BROTLI`ZSTD`LZ4`LZO`BZ2)!til 8

///
// Sets the compression codec for writing files
// @param codec Sym from .pq.codecs
.pq.setCodec:{[codec]
    if[not -11h~type codec;
        '"Codec must be a sym"];
    if[not codec in key .pq.codecs;
        '"Codec must exist in .pq.codecs"];
    .pq.priv.codec:.pq.codecs[codec];
 }

// Set defaults codec to ZSTD
.pq.setCodec[`ZSTD]

///
// Returns the compression the codec is set to
// @return sym The codec
.pq.getCodec:{.pq.codecs?.pq.priv.codec}

///
// Write a table to a parquet file
// @param table Table to write, use select from t for splayed tables
// @param filePath Filepath as a Sym/string, doesn't support hysm
// @param single Write a single rowgroup, or keep file handle open for future row groups
// @param codec Codec to compress the file with, see .pq.codecs
// @param append Append to existing file, otherwise truncate to 0 bytes
.pq.priv.write:.pq.priv.libPath 2:(`writer;5)

///
// Write a table to a parquet file
// Defaults to single row group, and doesn't append to existing
// @param t Table to write
// @param f Filepath as a Sym/string, doesn't support hysm
.pq.write:{[t;f]
    .pq.priv.write[select from t;f;1b;.pq.priv.codec; 0b]
 }

///
// Write a table to a parquet file
// Defaults to multi row group, and doesn't append to existing
// @param t Table to write
// @param f Filepath as a Sym/string, doesn't support hysm
.pq.writeMulti:{[t;f]
    .pq.priv.write[select from t;f;0b;.pq.priv.codec; 0b]
 }

///
// Close the loaded parquet file
.pq.closeWriter:.pq.priv.libPath 2:(`closeW;1) 