#ifndef KDB_UTILS
#define KDB_UTILS

#include <k.h>
#include <cmath>
#include <iostream>
#include <limits>

//Undefine these fro k.h to use a C++ style equivalent.
#undef kC
#undef kU

typedef bool    B;
typedef int64_t J64;

extern"C"{
std::string k2string(K x);
}

inline const B* kB(const k0* x){ return reinterpret_cast<const B*>(x->G0); }
inline const C* kC(const k0* x){ return reinterpret_cast<const C*>(x->G0); }
inline const J64* kJ64(const k0* x){ return reinterpret_cast<const J64*>(x->G0); }
#if KXVER>=3
inline U* kU(K x){return reinterpret_cast<U*>(x->G0);}
#endif

inline B* kB(K x){ return reinterpret_cast<B*>(x->G0); }
inline C* kC(K x){ return reinterpret_cast<C*>(x->G0); }
inline J64* kJ64(K x){ return reinterpret_cast<J64*>(x->G0); }
#if KXVER>=3
inline const U* kU(const k0* x){return reinterpret_cast<const U*>(x->G0);}
#endif

inline K kerror (const char* text) { return krr(const_cast<S>(text)); }
inline K kstring (const char* text) { return kp(const_cast<S>(text)); }
inline K kstring (const std::string &text) { return kpn(const_cast<S>(&text[0]), text.length()); }
inline K kstringn (const char* text, J len) { return kpn(const_cast<S>(text),len); }
inline K ksymbol (const char* text) { return ks(const_cast<S>(text)); }
inline K oerror (const char* text) { return orr(const_cast<S>(text)); }
inline S symbol (const char* text) { return ss(const_cast<S>(text)); }

#endif

