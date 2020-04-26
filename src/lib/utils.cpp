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

#include <utils.hpp>

std::string k2string(K x){
    switch(x->t){
        case -KS:
            return x->s;
        case -KC:
            return std::string {static_cast<char>(x->g),1};
        case KC:
            return std::string {kC(x), static_cast<size_t>(x->n)};
        default:
            return std::string {};
    }
}

std::vector<std::string> k2StrVec(K x){
    int len = x->n;
    std::vector<std::string> vec;
    if(x->t == KS){
        for(int i=0;i<len;i++)
            vec.push_back(kS(x)[i]);
    }else{
        for(int i=0;i<len;i++)
            vec.push_back(k2string(kK(x)[i]));
    }
    return vec;
}