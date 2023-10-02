#include <db/Catalog.h>
#include <db/TupleDesc.h>
#include <stdexcept>

using namespace db;

void Catalog::addTable(DbFile *file, const std::string &name, const std::string &pkeyField) {
    // TODO pa1.2: implement
    Table table(file, name, pkeyField);
    nameToTable.emplace(name, table);
    idToTable.emplace(file->getId(), table);
}

int Catalog::getTableId(const std::string &name) const {
    // TODO pa1.2: implement
    if (nameToTable.find(name) == nameToTable.end()) {
        throw std::invalid_argument("Not found");
    }
    return nameToTable.at(name).file->getId();
}

const TupleDesc &Catalog::getTupleDesc(int tableid) const {
    // TODO pa1.2: implement
    if (idToTable.find(tableid) == idToTable.end()) {
        throw std::invalid_argument("Not found");
    }
    return idToTable.at(tableid).file->getTupleDesc();
}

DbFile *Catalog::getDatabaseFile(int tableid) const {
    // TODO pa1.2: implement
    if (idToTable.find(tableid) == idToTable.end()) {
        throw std::invalid_argument("Not found");
    }
    return idToTable.at(tableid).file;
}

std::string Catalog::getPrimaryKey(int tableid) const {
    // TODO pa1.2: implement
    if (idToTable.find(tableid) == idToTable.end()) {
        throw std::invalid_argument("Not found");
    }
    return idToTable.at(tableid).pkeyField;
}

std::string Catalog::getTableName(int id) const {
    // TODO pa1.2: implement
    if (idToTable.find(id) == idToTable.end()) {
        throw std::invalid_argument("Not found");
    }
    return idToTable.at(id).name;
}

void Catalog::clear() {
    // TODO pa1.2: implement
    nameToTable.clear();
    idToTable.clear();
}
