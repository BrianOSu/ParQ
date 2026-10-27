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

#ifndef KDB_KOWNER
#define KDB_KOWNER

#include <k.h>

/**
 * Holds one q K value and releases its reference with r0 when the owner leaves
 * scope. Use it for temporary K values so early returns and exceptions do not
 * leak them. Copying is disabled because a K value must have only one owner.
 *
 * Call get() to borrow the value, or address() when a q API needs to write a K
 * result into it. Call release() to transfer ownership out; for example, pass
 * release() to a q API that takes ownership of its argument. After release(),
 * this object no longer calls r0 for that value.
 */
class KOwner {
public:
    explicit KOwner(K value = nullptr) noexcept : value_(value) {}
    ~KOwner() noexcept { if (value_) r0(value_); }

    KOwner(const KOwner&) = delete;
    KOwner& operator=(const KOwner&) = delete;

    K get() const noexcept { return value_; }
    K* address() noexcept { return &value_; }
    K release() noexcept {
        K value = value_;
        value_ = nullptr;
        return value;
    }

private:
    K value_;
};

#endif
