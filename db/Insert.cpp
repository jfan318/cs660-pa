#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    // TODO pa3.3: some code goes here
    if (this->isUsed) {
        return Tuple();
    }

    int count = 0;
    while (this->child->hasNext()) {
        Tuple tuple = this->child->next();
        Database::getBufferPool().insertTuple(this->t, this->tableId, &tuple);
        ++count;
    }

    this->isUsed = true;
    Tuple tp(this->td);
    tp.setField(0, reinterpret_cast<const Field *>(count));
    return tp;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId):
                t(t), child(child), tableId(tableId){
    // TODO pa3.3: some code goes here
    std::vector<Types::Type> typeAr = {Types::INT_TYPE};
    std::vector<std::string> fieldAr = {"INSERT_CNT"};
    this->td = TupleDesc(typeAr, fieldAr);

    this->isUsed = true;
    this->children = {this->child};
}

const TupleDesc &Insert::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return this->td;
}

void Insert::open() {
    // TODO pa3.3: some code goes here
    this->child->open();
    this->isUsed = false;
}

void Insert::close() {
    // TODO pa3.3: some code goes here
    this->child->close();
}

void Insert::rewind() {
    // TODO pa3.3: some code goes here
    this->child->rewind();
    this->isUsed = false;
}

std::vector<DbIterator *> Insert::getChildren() {
    // TODO pa3.3: some code goes here
    return this->children;
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    this->children = children;
}
