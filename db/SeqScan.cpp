#include <db/SeqScan.h>

using namespace db;

// TODO pa1.6: implement
SeqScan::SeqScan(TransactionId *tid, int tableid, const std::string &tableAlias) {
    this->tid = *tid;
    reset(tableid, tableAlias);
}

std::string SeqScan::getTableName() const {
    // TODO pa1.6: implement
    return Database::getCatalog().getTableName(this->tableid);
}

std::string SeqScan::getAlias() const {
    // TODO pa1.6: implement
    return this->tableAlias;
}

void SeqScan::reset(int tabid, const std::string &tableAlias) {
    // TODO pa1.6: implement
    this->tableid = tabid;
    this->tableAlias = tableAlias;

    this->tableName = Database::getCatalog().getTableName(tableid);
    this->td = Database::getCatalog().getTupleDesc(tableid);

    std::vector<std::string> fieldNames(td.numFields());
    std::vector<Types::Type> fieldTypes(td.numFields());

    for (int i=0; i<td.numFields(); i++) {
        std::string fieldname = td.getFieldName(i);
        Types::Type fieldtype = td.getFieldType(i);

        fieldNames[i] = tableAlias + "." + fieldname;
        fieldTypes[i] = fieldtype;
    }

    this->td = TupleDesc(fieldTypes, fieldNames);
}

const TupleDesc &SeqScan::getTupleDesc() const {
    // TODO pa1.6: implement
    return Database::getCatalog().getTupleDesc(tableid);
}

SeqScan::iterator SeqScan::begin() const {
    // TODO pa1.6: implement
    HeapPage* page = dynamic_cast<HeapPage *>(db::Database::getCatalog().getDatabaseFile(tableid)->readPage(
            HeapPageId(tableid, 0)));
    if(!page) {
        throw std::runtime_error("Invalid page or type mismatch.");
    }
    return SeqScanIterator(page, false);
}

SeqScan::iterator SeqScan::end() const {
    // TODO pa1.6: implement
    HeapPage* page = dynamic_cast<HeapPage *>(db::Database::getCatalog().getDatabaseFile(tableid)->readPage(
            HeapPageId(tableid, 0)));
    if(!page) {
        throw std::runtime_error("Invalid page or type mismatch.");
    }
    return SeqScanIterator(page, true);
}

//
// SeqScanIterator
//

// TODO pa1.6: implement
SeqScanIterator::SeqScanIterator(/* TODO pa1.6: add parameters */HeapPage* pg, bool isEnd = false)
        : page(pg),
          pageIter(isEnd ? pg->end() : pg->begin()) {
}

bool SeqScanIterator::operator!=(const SeqScanIterator &other) const {
    // TODO pa1.6: implement
    return pageIter != other.pageIter;
}

SeqScanIterator &SeqScanIterator::operator++() {
    // TODO pa1.6: implement
    ++pageIter;
    return *this;
}

const Tuple &SeqScanIterator::operator*() const {
    // TODO pa1.6: implement
    return *pageIter;
}