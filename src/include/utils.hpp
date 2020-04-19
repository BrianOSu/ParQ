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

#ifndef KDB_UTILS
#define KDB_UTILS

#include <k.h>
#include <iostream>

//Undefine these fro k.h to use a C++ style equivalent.
#ifdef KX
#undef kC
#undef kU
#endif

typedef bool    B;
typedef int64_t J64;

extern"C"{
    std::string k2string(K x);
}

inline const B* kB(const k0* x){ return reinterpret_cast<const B*>(x->G0); }
inline const C* kC(const k0* x){ return reinterpret_cast<const C*>(x->G0); }
inline const J64* kJ64(const k0* x){ return reinterpret_cast<const J64*>(x->G0); }
#if KXVER>=3
inline const U* kU(const k0* x){return reinterpret_cast<const U*>(x->G0);}
#endif

inline B* kB(K x){ return reinterpret_cast<B*>(x->G0); }
inline C* kC(K x){ return reinterpret_cast<C*>(x->G0); }
inline J64* kJ64(K x){ return reinterpret_cast<J64*>(x->G0); }
#if KXVER>=3
inline U* kU(K x){return reinterpret_cast<U*>(x->G0);}
#endif

inline K kerror (const char* text) { return krr(const_cast<S>(text)); }
inline K kerror (std::string text) { return krr(const_cast<S>(text.c_str())); }

#endif

