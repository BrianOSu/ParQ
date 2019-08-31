/////////////////////////////////////////////////////
//ParQ API to read/write parquet files from within q
//Author: Brian O'Sullivan
//Email: b.osullivan@live.ie
/////////////////////////////////////////////////////

///
// Set the path to the .so file
.pq.priv.libPath:`$"install/ParQ"

///
// Opens and reads the first row group
// in the supplied parquet file.
// @param file string/sym
// @param group long
.pq.readGroup:.pq.priv.libPath 2:(`readGroup;2) 

///
// Opens and reads the first row group
// in the supplied parquet file.
// @param file string/sym
.pq.read:.pq.readGroup[;0j]

///
// Load the parquet file to make it ready for reading
// @param file string/sym
.pq.load:.pq.priv.libPath 2:(`initReader;1) 

///
// Read the current row group for loaded file
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
// Write a table to a parquet file
// @param table The table the write down to parquet
// @param filePath Sym/string
.pq.write:.pq.priv.libPath 2:(`writerSingle;2)

///
// Write a table to a parquet file
// @param table The table the write down to parquet
// @param filePath Sym/string
.pq.writeMulti:.pq.priv.libPath 2:(`writer;2)

///
// Close the loaded parquet file
.pq.closeWriter:.pq.priv.libPath 2:(`closeW;1) 