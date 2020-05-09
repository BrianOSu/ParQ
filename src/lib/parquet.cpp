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

PKDB* PKDB::instance;

PKDB::PKDB(std::shared_ptr<parquet::ParquetFileReader> filerReader)
        : filerReader_(filerReader)
{
    //When opening files, point at the first row group
    currentRowGroup=0;
    row_group_reader=filerReader_->RowGroup(currentRowGroup);
    key_value_metadata=filerReader_->metadata()->key_value_metadata();
    totalRowGroups=filerReader_->metadata()->num_row_groups();
}

PKDB::~PKDB(){
    //Close the FD
    filerReader_->Close();
}

K PKDB::loadReader(std::string fileName){
    try {
        if(instance) instance->~PKDB();
        instance = new PKDB { PREADER::open_reader(fileName.c_str()) };
        updateMetaData();
        return kb(1);
    } catch (const std::exception& e) {
            char* error = const_cast<char*>(e.what());
            return orr(error);
    }
}

K PKDB::readGroup(std::string fileName, int group, K cols){
    try {
        std::shared_ptr<parquet::ParquetFileReader> filerReader = PREADER::open_reader(fileName.c_str());
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = filerReader->RowGroup(group);
        return readTable(row_group_reader,
						 cols->n ? cols->n : row_group_reader->metadata()->num_columns(),
						 row_group_reader->metadata()->num_rows(),
						 cols);
    } catch (const std::exception& e) {
            char* error = const_cast<char*>(e.what());
            return orr(error);
    }
}

void PKDB::updateMetaData(){
    instance->metaData=instance->row_group_reader->metadata();
    instance->numColumns=instance->metaData->num_columns();
    instance->numRows=instance->metaData->num_rows();
}

K PKDB::readTable(std::shared_ptr<parquet::RowGroupReader> row_group_reader,
                                         int num_cols,
                                         int num_rows,
                                         K cols){
    K colNames = ktn(KS,0);
    K colValues = ktn(0,num_cols);
    std::vector<std::thread> threads;
    int state = setm(1);

    for(int i=0; i<num_cols; i++){
        int index = cols->n ? getColIndex(row_group_reader, std::string(kS(cols)[i])) : i;
        if(index < 0) return krr(kS(cols)[i]);
        js(&colNames, PKDB::readColName(row_group_reader, index));
        threads.push_back(std::thread(&PKDB::appendCol, std::ref(kK(colValues)[i]), row_group_reader, index, num_rows));
    }

    for(int i=0; i<num_cols; i++){
        threads[i].join();
    }
    
    setm(state);

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

void PKDB::appendCol(K &col, std::shared_ptr<parquet::RowGroupReader> row_group_reader, int index, int num_rows){
    col = r1(PKDB::getColData(row_group_reader, index, num_rows));
    //m9(); <- m9 breaks returning all data. This causes a memory leak
}

K PKDB::close(){
    if(instance) instance->~PKDB();
    instance=nullptr;
    return kb(1);
}

PWRITE* PWRITE::instance;

PWRITE::PWRITE(std::shared_ptr<parquet::ParquetFileWriter> fileWriter)
        : fileWriter_(fileWriter)
{
    //When opening files, point at the first row group
    currentRowGroup=0;
}

PWRITE::~PWRITE(){
    //Close the FD
    fileWriter_->Close();
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
            instance = new PWRITE {file_writer};

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
    if(instance) instance->~PWRITE();
    instance=nullptr;
    return kb(1);
}