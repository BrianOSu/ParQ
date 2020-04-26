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
// Functions to read parquet files come in two flavours:
//   * Directly reading a rowgroup from a file without loading it
//   * Loading a file to read and extract information from it
//////////////////////////////////////////////////////////////////////////////


//////////////////////////////////////////////////////////////////////////////
// Directly reading parquet files
//////////////////////////////////////////////////////////////////////////////

///
// Opens and reads a given row group in the supplied parquet file.
// @param  File  - String/sym 
// @param  Group - Long representing the rowgroup to read
// @param  Cols  - Sym list representing columns to read, or (::) for all
// @return Table - Data extracted from the parquet file
.pq.readGroup:.pq.priv.libPath 2:(`readGroup;3) 

///
// Opens and reads the first row group in the supplied parquet file.
// @param  File  - String/sym
// @return Table - Data extracted from the parquet file
.pq.read:.pq.readGroup[;0j;(::)]



//////////////////////////////////////////////////////////////////////////////
// Loading parquet file and additional functions
//////////////////////////////////////////////////////////////////////////////

///
// Load a given parquet file to make it available for reading
// and extracting certain information from it
// @param  File - String/sym
// @return Bool - 1b if loads, otherwise throws error
.pq.load:.pq.priv.libPath 2:(`initReader;1) 

///
// Close the currently loaded parquet file
// @return Bool - 1b if closes, otherwise throws error
.pq.close:.pq.priv.libPath 2:(`closeP;1)

///
// Returns the current row group being pointed at by the loaded file
// @return Long - Row group,  otherwise throws error
.pq.rowGroup:.pq.priv.libPath 2:(`currentRowGroup;1) 

///
// Moves to next row group for reading
// @return Bool - 1b if moves, otherwise throws error
.pq.next:.pq.priv.libPath 2:(`nextRowGroup;1)

///
// Returns the total number of row groups in the loaded file
// @return Long - Total row groups,  otherwise throws error
.pq.totalRowGroup:.pq.priv.libPath 2:(`totalRowGroup;1) 

///
// Reads the current row group for loaded file
// @param  Cols  - Sym list representing columns to read. 
//                 (::) or left blank returns all cols
// @return Table - Data extracted from the parquet file
.pq.readMulti:.pq.priv.libPath 2:(`readMulti;1) 

///
// Returns the schema from the currently loaded parquet file
// @return String list - The schema coming directly from the parquet file
.pq.priv.schema:.pq.priv.libPath 2:(`readSchema;1)
.pq.schema:{[] -1_"\n" vs .pq.priv.schema[]}