#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    // TODO pa3.2: some code goes here
    if (this->aggrIter != nullptr && this->aggrIter->hasNext()) {
        return this->aggrIter->next();
    }
    return std::nullopt;
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop) :
                    child(child), afield(afield), gfield(gfield), aop(aop){
    // TODO pa3.2: some code goes here
    this->children = {child};
    TupleDesc temp = child->getTupleDesc();
    this->aFieldType = temp.getFieldType(afield);

    std::string aggLabel = to_string(aop) + "(" + temp.getFieldName(afield) + ")";

    std::vector<Types::Type> tpType;
    std::vector<std::string> tpLabel;

    if (gfield != Aggregator::NO_GROUPING) {
        this->gFieldType = temp.getFieldType(gfield);
        tpType = {&gFieldType, &aFieldType};
        tpLabel = {temp.getFieldName(gfield), aggLabel};
    } else {
        this->gFieldType = Types::Type();
        tpType = {this->aFieldType};
        tpLabel = {aggLabel};
    }

    this->mergeTd = new TupleDesc(tpType, tpLabel);
}

int Aggregate::groupField() {
    // TODO pa3.2: some code goes here
    return this->gfield;
}

std::string Aggregate::groupFieldName() {
    // TODO pa3.2: some code goes here
    if (this->gfield != Aggregator::NO_GROUPING) {
        return this->child->getTupleDesc().getFieldName(this->gfield);
    } else {
        return "";
    }
}

int Aggregate::aggregateField() {
    // TODO pa3.2: some code goes here
    return this->afield;
}

std::string Aggregate::aggregateFieldName() {
    // TODO pa3.2: some code goes here
    TupleDesc td = this->child->getTupleDesc();
    assert(this->afield >= 0);
    assert(this->afield < td.numFields());
    return td.getFieldName(this->afield);
}

Aggregator::Op Aggregate::aggregateOp() {
    // TODO pa3.2: some code goes here
    return this->aop;
}

void Aggregate::open() {
    // TODO pa3.2: some code goes here
    this->child->open();
    Aggregator* aggregator;

    if (aFieldType == Types::INT_TYPE) {
        aggregator = new IntegerAggregator(gfield, gFieldType, afield, aop);
    }
    else {
        aggregator = new StringAggregator(gfield, gFieldType, afield, aop);
    }

    while (this->child->hasNext()) {
        Tuple temp = this->child->next();
        aggregator->mergeTupleIntoGroup(&temp);
    }
    aggrIter = aggregator->iterator();
    aggrIter->open();
    Operator::open();
}

void Aggregate::rewind() {
    // TODO pa3.2: some code goes here
    if (!this->aggrIter) {
        this->aggrIter->rewind();
    } else {
        this->close();
        this->open();
    }
}

const TupleDesc &Aggregate::getTupleDesc() const {
    // TODO pa3.2: some code goes here
    return *this->mergeTd;
}

void Aggregate::close() {
    // TODO pa3.2: some code goes here
    this->child->close();
    this->aggrIter = nullptr;
    Operator::close();
}

std::vector<DbIterator *> Aggregate::getChildren() {
    // TODO pa3.2: some code goes here
    return this->children;
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.2: some code goes here
    this->children = children;
}
