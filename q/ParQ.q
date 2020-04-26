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

//////////////////////////////////////////////////////
// ParQ API to read/write parquet files from within q
// Author: Brian O'Sullivan
// Email: b.osullivan@live.ie
//////////////////////////////////////////////////////

///
// Set the path to the .so file
.pq.priv.libPath:`$"install/ParQ"

///
// Set the directory of the ParQ.q file location
.pq.priv.dir:"/"sv -1_"/"vs(reverse value {})2;

///
// Load the reader and writer functions
.pq.priv.load:{system"l ",.pq.priv.dir,"/",x}
.pq.priv.load"reader.q"
.pq.priv.load"writer.q"