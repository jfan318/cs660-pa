#include <db/IndexPredicate.h>

using namespace db;

IndexPredicate::IndexPredicate(Op op, const Field *fvalue) {
    // TODO pa2.2: implement
    this->op = op,
    this->fvalue = fvalue;
}

const Field *IndexPredicate::getField() const {
    // TODO pa2.2: implement
    return this->fvalue;
}

Op IndexPredicate::getOp() const {
    // TODO pa2.2: implement
    return this->op;
}

bool IndexPredicate::operator==(const IndexPredicate &other) const {
    // TODO pa2.2: implement
    return *fvalue == *other.getField() && op == other.getOp();
}

std::size_t std::hash<IndexPredicate>::operator()(const IndexPredicate &ipd) const {
    // TODO pa2.2: implement
    std::hash<const Field *> predicateHash;
    std::size_t h1 = std::hash<int>{}(static_cast<int>(ipd.getOp()));
    std::size_t h2 = predicateHash(ipd.getField());
    return h1 ^ h2;
}
