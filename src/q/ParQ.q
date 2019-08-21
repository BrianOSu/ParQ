/////////////////////////////////////////////////////
//ParQ API to read/write parquet files from within q
//Author: Brian O'Sullivan
//Email: b.osullivan@live.ie
/////////////////////////////////////////////////////

///
// Opens and reads the first row group
// in the supplied parquet file.
// @param file string/sym
// @param group long
.pq.readGroup:`ParQ 2:(`readGroup;2) 

///
// Opens and reads the first row group
// in the supplied parquet file.
// @param file string/sym
.pq.read:.pq.readGroup[;0j]

///
// Load the parquet file to make it ready for reading
// @param file string/sym
.pq.load:`ParQ 2:(`initReader;1) 

///
// Read the current row group for loaded file
.pq.readMulti:`ParQ 2:(`readMulti;1) 

///
// Move to next row group for reading
.pq.next:`ParQ 2:(`nextRowGroup;1) 

///
// Close the loaded parquet file
.pq.close:`ParQ 2:(`closeP;1) 

///
// The row group being pointed at for loaded file
.pq.rowGroup:`ParQ 2:(`currentRowGroup;1) 

///
// The total number of row groups in the loaded file
.pq.totalRowGroup:`ParQ 2:(`totalRowGroup;1) 

///
// Displa the parquet schema as string for loaded file
.pq.priv.schema:`ParQ 2:(`readSchema;1)
.pq.schema:{[] -1_"\n" vs .pq.priv.schema[]}

///
// Write a table to a parquet file
// @param table The table the write down to parquet
// @param filePath Sym/string
.pq.write:`ParQ 2:(`writerSingle;2)

///
// Write a table to a parquet file
// @param table The table the write down to parquet
// @param filePath Sym/string
.pq.writeMulti:`ParQ 2:(`writer;2)

///
// Close the loaded parquet file
.pq.closeWriter:`ParQ 2:(`closeW;1) 