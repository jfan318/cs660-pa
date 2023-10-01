#include <db/TupleDesc.h>
#include <functional>

using namespace db;

//
// TDItem
//

bool TDItem::operator==(const TDItem &other) const {
    // TODO pa1.1: implement
    return this->fieldName == other.fieldName && this->fieldType == other.fieldType;
}

std::size_t std::hash<TDItem>::operator()(const TDItem &r) const {
    // TODO pa1.1: implement
    return std::hash<std::string>()(r.fieldName) ^ std::hash<int>()(static_cast<int>(r.fieldType));
}

//
// TupleDesc
//

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    for (const auto &type : types) {
        fields.emplace_back(type, "");
    }
}

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    for (int i = 0; i < types.size(); i++) {
        fields.emplace_back(types[i], names[i]);
    }
}

size_t TupleDesc::numFields() const {
    // TODO pa1.1: implement
    return fields.size();
}

std::string TupleDesc::getFieldName(size_t i) const {
    // TODO pa1.1: implement
    return fields[i].fieldName;
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    // TODO pa1.1: implement
    return fields[i].fieldType;
}

int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    // TODO pa1.1: implement
    for (int i = 0; i < fields.size(); i++) {
        if (fields[i].fieldName == fieldName) {
            return i;
        }
    }
    return -1;
}

size_t TupleDesc::getSize() const {
    // TODO pa1.1: implement
    return fields.size() * sizeof(int);
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // TODO pa1.1: implement
    TupleDesc merged;
    merged.fields.insert(merged.fields.end(), td1.fields.begin(), td1.fields.end());
    merged.fields.insert(merged.fields.end(), td2.fields.begin(), td2.fields.end());
    return merged;
}

std::string TupleDesc::to_string() const {
    // TODO pa1.1: implement
    std::string desc = "";
    for (const auto &field : fields) {
        if (!desc.empty()) {
            desc += ", ";
        }
        desc += field.to_string();
    }
    return desc;
}

bool TupleDesc::operator==(const TupleDesc &other) const {
    // TODO pa1.1: implement
    return this->fields == other.fields;
}

TupleDesc::iterator TupleDesc::begin() const {
    // TODO pa1.1: implement
    return fields.begin();
}

TupleDesc::iterator TupleDesc::end() const {
    // TODO pa1.1: implement
    return fields.end();
}

std::size_t std::hash<db::TupleDesc>::operator()(const db::TupleDesc &td) const {
    // TODO pa1.1: implement
    size_t hash_value = 0;
    for (const auto &field : td) {
        hash_value ^= std::hash<TDItem>()(field);
    }
    return hash_value;
}
