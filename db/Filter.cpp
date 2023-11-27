#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child) : p(p), child(child){
    // TODO pa3.1: some code goes here
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    return &this->p;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return this->child->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    this->child->rewind();
    Operator::open();
}

void Filter::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    open();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    return this->children;
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    this->children = children;
}

std::optional<Tuple> Filter::fetchNext() {
    // TODO pa3.1: some code goes here
    Tuple temp;

    while (this->child->hasNext()) {
        temp = this->child->next();

        if (this->p.filter(temp)) {
            return temp;
        }
    }
    return std::nullopt;
}
