#include <kowner.hpp>
#include <qforeign.hpp>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace {

struct Payload {
    explicit Payload(int value) : value(value) {}
    int value;
};

int destroyedPayloads = 0;

extern "C" void destroyPayload(K handle) noexcept {
    auto* payload = reinterpret_cast<Payload*>(kK(handle)[1]);
    kK(handle)[1] = nullptr;
    delete payload;
    ++destroyedPayloads;
}

extern "C" void otherDestructor(K) noexcept {}

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void testKOwnerScopeCleanup() {
    K value = ktn(KG, 1);
    require(value != nullptr, "q allocation should succeed");
    r1(value); // Keep a reference so the allocation remains inspectable after KOwner releases its own.
    {
        KOwner owner{value};
        require(owner.get() == value, "KOwner should retain its K value");
        require(owner.address() != nullptr, "KOwner should expose the address of its K value");
    }
    require(value->r == 0, "KOwner should release its reference at scope exit");
    r0(value);
}

void testKOwnerReleaseTransfersOwnership() {
    K raw = nullptr;
    {
        K value = ktn(KG, 1);
        require(value != nullptr, "q allocation should succeed");
        KOwner owner{value};
        raw = owner.release();
        require(raw == value, "release should return the owned K value");
        require(owner.get() == nullptr, "release should clear the owner");
    }
    r0(raw);
}

void testKOwnerAddressCanReceiveAValue() {
    KOwner owner;
    *owner.address() = ktn(KG, 1);
    require(owner.get() != nullptr, "address should allow an API result to be stored");
}

void testForeignObjectRoundTripAndDestructor() {
    destroyedPayloads = 0;
    K handle = makeQForeignObject(std::make_unique<Payload>(19), &destroyPayload);
    require(handle->t == 112, "foreign objects should use q type 112");
    require(handle->n == 2, "foreign objects should contain two slots");
    require(qForeignObjectState<Payload>(handle, &destroyPayload)->value == 19,
            "foreign object lookup should return the native state");
    require(kK(handle)[0] == reinterpret_cast<K>(&destroyPayload),
            "foreign object should store its destructor callback");
    // c.o does not execute q's type 112 finalizer; invoke the callback explicitly
    // so this standalone test can still verify and clean up the native payload.
    destroyPayload(handle);
    r0(handle);
    require(destroyedPayloads == 1, "the stored foreign-object callback should destroy its payload");
}

void testForeignObjectRejectsInvalidHandles() {
    destroyedPayloads = 0;
    bool rejectedNull = false;
    try {
        qForeignObjectState<Payload>(nullptr, &destroyPayload);
    } catch (const std::invalid_argument&) {
        rejectedNull = true;
    }
    require(rejectedNull, "null handles should be rejected");

    K wrongType = ktn(0, 2);
    require(wrongType != nullptr, "q list allocation should succeed");
    bool rejectedWrongType = false;
    try {
        qForeignObjectState<Payload>(wrongType, &destroyPayload);
    } catch (const std::invalid_argument&) {
        rejectedWrongType = true;
    }
    r0(wrongType);
    require(rejectedWrongType, "non-foreign q values should be rejected");

    K wrongDestructor = makeQForeignObject(std::make_unique<Payload>(0), &destroyPayload);
    bool rejectedWrongDestructor = false;
    try {
        qForeignObjectState<Payload>(wrongDestructor, &otherDestructor);
    } catch (const std::invalid_argument&) {
        rejectedWrongDestructor = true;
    }
    destroyPayload(wrongDestructor);
    r0(wrongDestructor);
    require(rejectedWrongDestructor, "handles with a different destructor should be rejected");
    require(destroyedPayloads == 1, "rejected handles should remain safely releasable");
}

} // namespace

int main() {
    try {
        testKOwnerScopeCleanup();
        testKOwnerReleaseTransfersOwnership();
        testKOwnerAddressCanReceiveAValue();
        testForeignObjectRoundTripAndDestructor();
        testForeignObjectRejectsInvalidHandles();
        std::cout << "KOwner and qforeign tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "KOwner and qforeign tests failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
