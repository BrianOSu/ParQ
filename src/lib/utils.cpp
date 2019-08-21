#include <utils.hpp>

std::string k2string(K x){
    switch(x->t){
        case -KS:
            return x->s;
        case KC:
            return std::string {kC(x),static_cast<size_t>(x->n)};
        default:
            return std::string {};
    }
}