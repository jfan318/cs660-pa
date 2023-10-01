#include <db/Tuple.h>

using namespace db;

//
// Tuple
//

// TODO pa1.1: implement
Tuple::Tuple(const TupleDesc &td, RecordId *rid) {
    this->tupleDesc=td;
    this->recordId=rid;
    fields.resize(td.numFields());
}

const TupleDesc &Tuple::getTupleDesc() const {
    // TODO pa1.1: implement
    return this->tupleDesc;
}

const RecordId *Tuple::getRecordId() const {
    // TODO pa1.1: implement
    return this->recordId;
}

void Tuple::setRecordId(const RecordId *id) {
    // TODO pa1.1: implement
    this->recordId = id;
}

const Field &Tuple::getField(int i) const {
    // TODO pa1.1: implement
    if (i < 0 || i >= fields.size()) {
        throw std::out_of_range("Invalid field index");
    }
    return *fields[i];
}

void Tuple::setField(int i, const Field *f) {
    // TODO pa1.1: implement
    this->fields[i].reset((Field*) &f);
}

Tuple::iterator Tuple::begin() const {
    // TODO pa1.1: implement
    return TupleIterator(0, fields);
}

Tuple::iterator Tuple::end() const {
    // TODO pa1.1: implement
    return TupleIterator(fields.size(), fields);

}

std::string Tuple::to_string() const {
    // TODO pa1.1: implement
    std::string result;
    for (const auto &field : fields) {
        if (!result.empty()) {
            result += ", ";
        }
        result += field->to_string();
    }
    return result;
}
