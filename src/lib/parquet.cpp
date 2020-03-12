#include <parquet.hpp>

using namespace KDB::PARQ;

PKDB* PKDB::instance;

PKDB::PKDB(std::shared_ptr<parquet::ParquetFileReader> filerReader)
    : filerReader_(filerReader)
{
  //When opening files, point at the first row group
  currentRowGroup=0;
  row_group_reader=filerReader_->RowGroup(currentRowGroup);
  totalRowGroups=filerReader_->metadata()->num_row_groups();
}

PKDB::~PKDB(){
  //Close the FD
  filerReader_->Close();
}

K PKDB::loadReader(std::string fileName){
  try {
    if(instance)instance->~PKDB();
    instance = new PKDB { PREADER::open_reader(fileName.c_str()) };
    updateMetaData();
    return kb(1);
  } catch (const std::exception& e) {
      char* error = const_cast<char*>(e.what());
      return orr(error);
  }
}

K PKDB::readGroup(std::string fileName, int group){
  try {
    std::shared_ptr<parquet::ParquetFileReader> filerReader = PREADER::open_reader(fileName.c_str());
    std::shared_ptr<parquet::RowGroupReader> row_group_reader = filerReader->RowGroup(group);
    return readTable(row_group_reader,
                     row_group_reader->metadata()->num_columns(),
                     row_group_reader->metadata()->num_rows());
    filerReader->Close();
  } catch (const std::exception& e) {
      char* error = const_cast<char*>(e.what());
      return orr(error);
  }
}

void PKDB::updateMetaData(){
  instance->metaData=instance->row_group_reader->metadata();
  instance->numColumns=instance->metaData->num_columns();
  instance->numRows=instance->metaData->num_rows();
};

K PKDB::readTable(std::shared_ptr<parquet::RowGroupReader> row_group_reader,
                     int num_cols,
                     int num_rows){
  //This will hold the column names
  K colNames=ktn(KS,num_cols);
  //This will hold column values
  K colValues = ktn(0,0);

  for(int i=0; i<num_cols; i++){
    std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(i);
    int fixedLengthByteSize = row_group_reader->metadata()->schema()->Column(i)->type_length();
    kS(colNames)[i]=ss(const_cast<char*>(row_group_reader->metadata()->schema()->Column(i)->name().c_str()));
    jk(&colValues,PREADER::readColumns(column_reader, num_rows, fixedLengthByteSize));
  }

  //Return a table to the process
  return xT(xD(colNames,colValues));
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
                                                        bool single){
  if(!instance || single){
    std::shared_ptr<GroupNode> schema = WRITER::SetupSchema(colNames, colValues, colValues->n);
    return WRITER::OpenFile(fileName, schema);
  } else{
      return instance->fileWriter_;
  }
}

K PWRITE::write(K table, std::string fileName, bool single){
  try{
    K colValues=kK(table->k)[1];
    K colNames=kK(table->k)[0];
    std::shared_ptr<parquet::ParquetFileWriter> file_writer = 
                    open_file_writer(colNames, colValues, fileName, single);
    if(!instance && !single) instance = new PWRITE {file_writer};
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