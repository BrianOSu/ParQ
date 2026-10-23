/*
   Copyright 2020 Brian O'Sullivan

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <parquet.hpp>

using namespace KDB::PARQ;

std::unique_ptr<PKDB> PKDB::instance{};

PKDB::PKDB(std::shared_ptr<parquet::ParquetFileReader> filerReader)
        : filerReader_(filerReader)
{
    //When opening files, point at the first row group
    row_group_reader_ = filerReader_->RowGroup(currentRowGroup_);
    key_value_metadata_ = filerReader_->metadata()->key_value_metadata();
    totalRowGroups_ = filerReader_->metadata()->num_row_groups();
    updateMetaData();
}

PKDB::~PKDB() noexcept {
    try {
        closeFile();
    } catch (...) {
        // Destructors must not let exceptions escape. The reader's shared
        // owner will still release its resources when this object is destroyed.
    }
}

void PKDB::closeFile() {
    if (!closed_ && filerReader_) {
        closed_ = true;
        filerReader_->Close();
    }
}

K PKDB::loadReader(std::string fileName){
    try {
        auto reader = std::make_unique<PKDB>(PREADER::open_reader(fileName.c_str()));
        instance = std::move(reader);
        return kb(1);
    } catch (const std::exception& e) {
            char* error = const_cast<char*>(e.what());
            return orr(error);
    }
}

K PKDB::readGroup(std::string fileName, int group, K cols) {
    try {
        std::shared_ptr<parquet::ParquetFileReader> filerReader = PREADER::open_reader(fileName.c_str());
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = filerReader->RowGroup(group);

        K result = readTable(
            row_group_reader,
            cols->n ? cols->n : row_group_reader->metadata()->num_columns(),
            row_group_reader->metadata()->num_rows(),
            cols
        );

        filerReader->Close();
        return result;

    } catch (const std::exception& e) {
        char* error = const_cast<char*>(e.what());
        return orr(error);
    }
}

void PKDB::updateMetaData(){
    metaData_ = row_group_reader_->metadata();
    numColumns_ = metaData_->num_columns();
    numRows_ = metaData_->num_rows();
}

K PKDB::readCurrentGroup(K cols) const {
    return readTable(row_group_reader_, cols->n ? cols->n : numColumns_, numRows_, cols);
}

bool PKDB::advanceRowGroup() {
    if (currentRowGroup_ >= totalRowGroups_ - 1) return false;
    ++currentRowGroup_;
    row_group_reader_ = filerReader_->RowGroup(currentRowGroup_);
    updateMetaData();
    return true;
}

std::string PKDB::schema() const {
    return metaData_->schema()->ToString();
}

std::shared_ptr<const parquet::KeyValueMetadata> PKDB::keyValueMetadata() const {
    return key_value_metadata_;
}

K PKDB::readTable(std::shared_ptr<parquet::RowGroupReader> row_group_reader,
                                         int num_cols,
                                         int num_rows,
                                         K cols){
    //This will hold the column names
    K colNames = ktn(KS,num_cols);
    //This will hold column values
    K colValues = ktn(0,0);

    for(int i=0; i<num_cols; i++){
        int index = cols->n ? getColIndex(row_group_reader, std::string{kS(cols)[i]}) : i;
		if(index < 0) return krr(kS(cols)[i]);
        kS(colNames)[i] = PKDB::readColName(row_group_reader, index);
        jk(&colValues, PKDB::getColData(row_group_reader, index, num_rows));
    }

    //Return a table to the process
    return xT(xD(colNames, colValues));
}

int PKDB::getColIndex(std::shared_ptr<parquet::RowGroupReader> row_group_reader, std::string colName){
    return row_group_reader->metadata()->schema()->ColumnIndex(colName);
}

S PKDB::readColName(std::shared_ptr<parquet::RowGroupReader> row_group_reader, int index){
    return ss(const_cast<char*>(row_group_reader->metadata()->schema()->Column(index)->name().c_str()));
}

K PKDB::getColData(std::shared_ptr<parquet::RowGroupReader> row_group_reader, int index, int num_rows){
	return PREADER::readColumns(row_group_reader->Column(index), num_rows);
}

K PKDB::close(){
    try {
        if (instance) instance->closeFile();
        instance.reset();
    } catch (...) {
        instance.reset();
        throw;
    }
    return kb(1);
}

std::unique_ptr<PWRITE> PWRITE::instance{};

PWRITE::PWRITE(std::shared_ptr<parquet::ParquetFileWriter> fileWriter)
        : fileWriter_(fileWriter)
{
    //When opening files, point at the first row group
    currentRowGroup_ = 0;
}

PWRITE::~PWRITE() noexcept {
    try {
        closeFile();
    } catch (...) {
        // Destructors must not let exceptions escape. Releasing the shared
        // writer owner still provides a final cleanup path.
    }
}

void PWRITE::closeFile() {
    if (!closed_ && fileWriter_) {
        closed_ = true;
        fileWriter_->Close();
    }
}

std::shared_ptr<parquet::ParquetFileWriter> PWRITE::open_file_writer(K colNames, 
                                                                     K colValues, 
                                                                     std::string fileName,
                                                                     bool single,
                                                                     parquet::Compression::type codec,
                                                                     bool append,
                                                                     K metadata){
    if(!instance || single)
        return WRITER::OpenFile(fileName, WRITER::SetupSchema(colNames, colValues, colValues->n),
                                codec, append, metadata);
    else
        return instance->fileWriter_;
}

K PWRITE::write(K table, std::string fileName, bool single, 
                parquet::Compression::type codec, bool append, K metadata){
    try{
        K colValues=kK(table->k)[1];
        K colNames=kK(table->k)[0];
        std::shared_ptr<parquet::ParquetFileWriter> file_writer = open_file_writer(colNames, colValues,
                                                                                   fileName, single,
                                                                                   codec, append, metadata);
        if(!instance && !single)
            instance = std::make_unique<PWRITE>(file_writer);

        parquet::RowGroupWriter* rg_writer = file_writer->AppendRowGroup();
        for(int i=0;i<colValues->n;i++)
                WRITER::writeColumn(kK(colValues)[i], rg_writer);

        if(single) file_writer->Close();
        return kb(1);
    }catch (const std::exception& e) {
            char* error = const_cast<char*>(e.what());
            return orr(error);
    }        
}

K PWRITE::close(){
    try {
        if (instance) instance->closeFile();
        instance.reset();
    } catch (...) {
        instance.reset();
        throw;
    }
    return kb(1);
}
