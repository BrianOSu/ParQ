#include <parquet.hpp>

using namespace KDB::PARQ;

extern"C"{
  K readGroup(K filename, K group){
    if(filename->t!=KC && filename->t!=-KS)
      return kerror("File name must be a string/symbol");
    if(group->t!=-KJ)
      return kerror("Group must be a long");
    return PKDB::readGroup(k2string(filename), group->j);
  }

  K initReader(K filename){
    if(filename->t!=KC && filename->t!=-KS)
      return kerror("File name must be a string/symbol");
    return PKDB::loadReader(k2string(filename));
  }

  K readMulti(K /*x*/){
    auto instance = &PKDB::getInstance();
    if(!instance) return kerror("Parquet file not loaded");
    try {
      return instance->readTable(instance->row_group_reader, instance->numColumns, instance->numRows);
    } catch (const std::exception& e) {
      return orr(const_cast<char*>(e.what()));
    }
  }

  K nextRowGroup(K /*x*/){
    auto instance = &PKDB::getInstance();
    if(!instance) return kerror("Parquet file not loaded");
    if(instance->currentRowGroup==instance->totalRowGroups-1)
      return kerror("Already at the latest row group");
    instance->incrementCurrentRowGroup();
    instance->row_group_reader=instance->filerReader_->RowGroup(instance->currentRowGroup);
    instance->PKDB::updateMetaData();
    return kb(1);
  }

  K readSchema(K /*x*/){
    auto instance = &PKDB::getInstance();
    if(!instance) return kerror("Parquet file not loaded");
    try {
      std::string schema = instance->metaData->schema()->ToString();
      return kp(const_cast<char*>(schema.c_str()));
    } catch (const std::exception& e) {
      return orr(const_cast<char*>(e.what()));
    }
  }

  K closeP(K /*x*/){
    auto instance = &PKDB::getInstance();
    if(!instance) return kerror("Parquet file not loaded");
    try {
      return instance->close();
    } catch (const std::exception& e) {
      return orr(const_cast<char*>(e.what()));
    }
  }

  K currentRowGroup(K /*x*/){
    auto instance = &PKDB::getInstance();
    if(!instance) return kerror("Parquet file not loaded");
    return kj(instance->currentRowGroup);
  }

  K totalRowGroup(K /*x*/){
    auto instance = &PKDB::getInstance();
    if(!instance) return kerror("Parquet file not loaded");
    return kj(instance->totalRowGroups);
  }

  K writer(K table, K filename){
    if(filename->t!=KC && filename->t!=-KS)
      return kerror("File name must be a string/symbol");
    return PWRITE::write(table, k2string(filename), false);
  }

  K writerSingle(K table, K filename){
    if(filename->t!=KC && filename->t!=-KS)
      return kerror("File name must be a string/symbol");
    return PWRITE::write(table, k2string(filename), true);
  }

  K closeW(K /*x*/){
    auto instance = &PWRITE::getInstance();
    if(!instance) return kerror("Parquet file not loaded");
    try {
      return instance->close();
    } catch (const std::exception& e) {
      return orr(const_cast<char*>(e.what()));
    }
  }
}