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

#ifndef KDB_QFOREIGN
#define KDB_QFOREIGN

#include <k.h>

#include <memory>
#include <stdexcept>

/**
 * q foreign objects (type 112) let an extension expose a native C++ object as
 * a q value. This helper stores a destructor callback and an opaque pointer in
 * the two slots of a general list, then marks the list as type 112. When q
 * releases the value, it calls the stored callback with that value.
 *
 * Each native object type supplies its own callback, which should clear the
 * pointer slot and destroy the pointed-to object. The callback must be
 * compatible with q's void(K) destructor convention.
 */
using QForeignDestructor = void (*)(K);

template <typename T>
K makeQForeignObject(std::unique_ptr<T> object, QForeignDestructor destructor) {
    if (!object || !destructor) {
        throw std::invalid_argument("A q foreign object needs a value and destructor");
    }

    K handle = ktn(0, 2);
    if (!handle) throw std::runtime_error("Failed to allocate q foreign object");

    kK(handle)[0] = reinterpret_cast<K>(destructor);
    kK(handle)[1] = reinterpret_cast<K>(object.get());
    handle->t = 112;
    object.release();
    return handle;
}

/**
 * Recover the native pointer from a type 112 value after checking that it has
 * the expected shape and destructor. The returned pointer is borrowed; q
 * retains ownership and may destroy it when the handle is released.
 */
template <typename T>
T* qForeignObjectState(K handle, QForeignDestructor expectedDestructor) {
    if (!expectedDestructor || !handle || handle->t != 112 || handle->n != 2 ||
        kK(handle)[0] != reinterpret_cast<K>(expectedDestructor)) {
        throw std::invalid_argument("Invalid q foreign object");
    }
    return reinterpret_cast<T*>(kK(handle)[1]);
}

#endif
