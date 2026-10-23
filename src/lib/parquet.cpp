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
#include <stdexcept>

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
        return kerror(e.what());
    }
}

K PKDB::readGroup(std::string fileName, int group, K cols) {
    try {
        std::shared_ptr<parquet::ParquetFileReader> filerReader = PREADER::open_reader(fileName.c_str());
        if (group < 0 || group >= filerReader->metadata()->num_row_groups()) {
            throw std::out_of_range("Row group index is out of range");
        }
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = filerReader->RowGroup(group);
        const bool selected = cols && cols->t == KS && cols->n > 0;

        KOwner result{readTable(
            row_group_reader,
            selected ? cols->n : row_group_reader->metadata()->num_columns(),
            row_group_reader->metadata()->num_rows(),
            cols
        )};

        filerReader->Close();
        return result.release();

    } catch (const std::exception& e) {
        return kerror(e.what());
    }
}

void PKDB::updateMetaData(){
    metaData_ = row_group_reader_->metadata();
    numColumns_ = metaData_->num_columns();
    numRows_ = metaData_->num_rows();
}

K PKDB::readCurrentGroup(K cols) const {
    const bool selected = cols && cols->t == KS && cols->n > 0;
    return readTable(row_group_reader_, selected ? cols->n : numColumns_, numRows_, cols);
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
    KOwner colNames{ktn(KS,num_cols)};
    //This will hold column values
    KOwner colValues{ktn(0,0)};
    if (!colNames.get() || !colValues.get()) {
        throw std::runtime_error("Failed to allocate q table columns");
    }

    for(int i=0; i<num_cols; i++){
		const bool selected = cols && cols->t == KS && cols->n > 0;
		int index = selected ? getColIndex(row_group_reader, std::string{kS(cols)[i]}) : i;
		if(index < 0) return krr(kS(cols)[i]);
        kS(colNames.get())[i] = PKDB::readColName(row_group_reader, index);
        KOwner column{PKDB::getColData(row_group_reader, index, num_rows)};
        if (!column.get()) throw std::runtime_error("Failed to allocate a q column");
        jk(colValues.address(), column.release());
    }

    //Return a table to the process
    KOwner dictionary{xD(colNames.release(), colValues.release())};
    if (!dictionary.get()) throw std::runtime_error("Failed to create q table dictionary");
    return xT(dictionary.release());
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
        return kerror(e.what());
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
