#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child) :
                t(t), child(child){
    // TODO pa3.3: some code goes here
    std::vector<Types::Type> typeAr = {Types::INT_TYPE};
    std::vector<std::string> fieldAr = {"INSERT_CNT"};
    this->cntTD = TupleDesc(typeAr, fieldAr);

    this->children = {child};
}

const TupleDesc &Delete::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return this->cntTD;
}

void Delete::open() {
    // TODO pa3.3: some code goes here
    this->child->open();
    this->used = false;
}

void Delete::close() {
    // TODO pa3.3: some code goes here
    this->child->close();
}

void Delete::rewind() {
    // TODO pa3.3: some code goes here
    this->child->rewind();
    this->used = false;
}

std::vector<DbIterator *> Delete::getChildren() {
    // TODO pa3.3: some code goes here
    return this->children;
}

void Delete::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    this->children = children;
}

std::optional<Tuple> Delete::fetchNext() {
    // TODO pa3.3: some code goes here
    if (this->used) {
        return Tuple();
    }

    int count = 0;
    while (this->child->hasNext()) {
        Tuple tuple = this->child->next();
        Database::getBufferPool().insertTuple(this->t, this->tableId, &tuple);
        ++count;
    }

    this->used = true;
    Tuple tp(this->cntTD);
    tp.setField(0, reinterpret_cast<const Field *>(count));
    return tp;
}
