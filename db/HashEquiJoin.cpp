#include <db/HashEquiJoin.h>
#include <db/Field.h>
#include <db/IntField.h>
#include <db/StringField.h>

using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2) :
                p(p), child1(child1), child2(child2){
    // TODO pa3.1: some code goes here
    this->mergeTd = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
    this->children = {child1, child2};
    this->tpIndex = 0;
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return &p;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return mergeTd;
}

std::string HashEquiJoin::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return this->child1->getTupleDesc().getFieldName(this->p.getField1());
}

std::string HashEquiJoin::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return this->child2->getTupleDesc().getFieldName(this->p.getField2());
}

void HashEquiJoin::open() {
    // TODO pa3.1: some code goes here
    child1->open();
    child2->open();

    if (keyType == Types::INT_TYPE) {
        intMap.clear();
    } else if (keyType == Types::STRING_TYPE) {
        strMap.clear();
    } else {
        throw std::runtime_error("Invalid Hashjoin Key type.");
    }

    Tuple readTuple;
    std::vector<Tuple> *list;
    while (child1->hasNext()) {
        readTuple = child1->next();
        IntField f = (IntField &&) readTuple.getField(p.getField1());

        if (keyType == Types::INT_TYPE) {
            IntField key = dynamic_cast<IntField&>(f);
            auto it = intMap.find(key.getValue());
            if (it != intMap.end()) {
                list = &(it->second);
            } else {
                list = &(intMap[key.getValue()]);
            }
            list->push_back(readTuple);
        } else {
            StringField key = dynamic_cast<StringField&>(f);
            auto it = strMap.find(key.getValue());
            if (it != strMap.end()) {
                list = &(it->second);
            } else {
                list = &(strMap[key.getValue()]);
            }
            list->push_back(readTuple);
        }
    }
    Operator::open();
}

void HashEquiJoin::close() {
    // TODO pa3.1: some code goes here
    this->child1->close();
    this->child2->close();

    this->intMap.clear();
    this->strMap.clear();
}

void HashEquiJoin::rewind() {
    // TODO pa3.1: some code goes here
    this->close();
    this->open();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    // TODO pa3.1: some code goes here
    return this->children;
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    this->children = children;
}

std::optional<Tuple> HashEquiJoin::fetchNext() {
    // TODO pa3.1: some code goes here
    if (!this->tuples.empty()) {
        tuple1 = tuples[tpIndex];
        tpIndex++;

        Tuple mergedTuple(mergeTd);
        for (int i = 0; i < tuple1.getTupleDesc().numFields(); ++i) {
            mergedTuple.setField(i, &tuple1.getField(i));
        }
        for (int i = 0; i < tuple2.getTupleDesc().numFields(); ++i) {
            mergedTuple.setField(i + tuple1.getTupleDesc().numFields(), &tuple2.getField(i));
        }
        return mergedTuple;

    }
    else {
        while (child2->hasNext()) {
            tuple2 = child2->next();
            IntField* f = (IntField *) &tuple2.getField(p.getField2());
            if (keyType == Types::INT_TYPE) {
                IntField key = *f;
                if (intMap.find(key.getValue()) != intMap.end()) {
                    tuples = this->intMap[key.getValue()];
                } else {
                    continue;
                }
            } else {
                StringField key = reinterpret_cast<StringField &&>(f);
                if (strMap.find(key.getValue()) != strMap.end()) {
                    tuples = this->strMap[key.getValue()];
                } else {
                    continue;
                }
            }
            if (tuples.empty()) {
                tuple1 = tuples[tpIndex];
                tpIndex++;
                Tuple mergedTuple(mergeTd);
                for (int i = 0; i < tuple1.getTupleDesc().numFields(); ++i) {
                    mergedTuple.setField(i, &tuple1.getField(i));
                }
                for (int i = 0; i < tuple2.getTupleDesc().numFields(); ++i) {
                    mergedTuple.setField(i + tuple1.getTupleDesc().numFields(), &tuple2.getField(i));
                }
                return mergedTuple;
            }
        }
    }
    return std::nullopt;
}
