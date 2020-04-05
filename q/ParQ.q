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
.pq.codecs:(`UNCOMPRESSED`SNAPPY`GZIP`LZO`BROTLI`LZ4`ZSTD)!til 7

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
// @param table The table the write down to parquet
// @param filePath The filepath as a Sym/string
// @param single Tells the writer if the file will have multiple row groups
// @param codec The codec to compress the file with
.pq.priv.write:.pq.priv.libPath 2:(`writer;4)

///
// Write a table to a parquet file
// @param t The table the write down to parquet
// @param f The filepath as a Sym/string
.pq.write:{[t;f]
    .pq.priv.write[select from t;f;1b;.pq.priv.codec]
 }

///
// Write a table to a parquet file
// @param t The table the write down to parquet
// @param f The filepath as a Sym/string
.pq.writeMulti:{[t;f]
    .pq.priv.write[select from t;f;0b;.pq.priv.codec]
 }

///
// Close the loaded parquet file
.pq.closeWriter:.pq.priv.libPath 2:(`closeW;1) 