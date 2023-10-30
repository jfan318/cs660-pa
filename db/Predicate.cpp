#include <db/Predicate.h>

using namespace db;

// TODO pa2.2: implement
Predicate::Predicate(int field, Op op, const Field *operand)
    : field(field), op(op), operand(operand) {}

int Predicate::getField() const {
    // TODO pa2.2: implement
    return this->field;
}

Op Predicate::getOp() const {
    // TODO pa2.2: implement
    return this->op;
}

const Field *Predicate::getOperand() const {
    // TODO pa2.2: implement
    return this->operand;
}

bool Predicate::filter(const Tuple &t) const {
    // TODO pa2.2: implement
    return {};
}
