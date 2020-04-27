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

extern"C"{
    K readGroup(K filename, K group, K cols){
        if(filename->t!=KC && filename->t!=-KS)
            return kerror("File name must be a string/symbol");
        if(group->t!=-KJ)
            return kerror("Group must be a long");
        if(cols->t!=KS && cols->n != 0)
            return kerror("Cols must be a list of symbols");
        return PKDB::readGroup(k2string(filename), group->j, cols);
    }

    K initReader(K filename){
        if(filename->t!=KC && filename->t!=-KS)
            return kerror("File name must be a string/symbol");
        return PKDB::loadReader(k2string(filename));
    }

    K readMulti(K cols){
        if(cols->t!=KS && cols->n != 0)
            return kerror("Cols must be a list of symbols");

        auto instance = &PKDB::getInstance();
        if(!instance)
			return kerror("Parquet file not loaded");

        try {
            return instance->readTable(instance->row_group_reader, 
                                    	cols->n ? cols->n : instance->numColumns, 
										instance->numRows, 
										cols);
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
            return string2k(instance->metaData->schema()->ToString());
        } catch (const std::exception& e) {
            return orr(const_cast<char*>(e.what()));
        }
    }

    K readKeyValueMetadata(K /*x*/){
        auto instance = &PKDB::getInstance();
        if(!instance) return kerror("Parquet file not loaded");
        try {
            K key = ktn(KS, 0);
            K value = ktn(KS, 0);
            if(instance->key_value_metadata){
                for(auto kv : instance->key_value_metadata->sorted_pairs()) {
                    js(&key,ss((char*)kv.first.c_str()));
                    js(&value,ss((char*)kv.second.c_str()));
                }
            }
            return xD(key,value);
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

    K writer(K table, K filename, K single, K codec, K metadata){
        if(filename->t!=KC && filename->t!=-KS)
            return kerror("File name must be a string/symbol");
        if(single->t!=-KB)
            return kerror("Single must be a bool");
        if(codec->t!=-KJ)
            return kerror("Codec must be a long");
        if(metadata->t!=XD && metadata->n != 0)
            return kerror("metadata must be a dictionary");
        return PWRITE::write(table, k2string(filename), single->g, 
                             parquet::Compression::type(codec->j), false, metadata);
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