#include <db/JoinPredicate.h>
#include <db/Tuple.h>
#include <db/Field.h>
#include <db/IntField.h>
#include <db/StringField.h>

using namespace db;

JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2):
                field1(field1), op(op), field2(field2){
    // TODO pa3.1: some code goes here
}

bool JoinPredicate::filter(Tuple *t1, Tuple *t2) {
    // TODO pa3.1: some code goes here
    if (!t1 || !t2) {
        return false;
    }

    if (t1->getField(field1).getType() == Types::INT_TYPE && t2->getField(field2).getType() == Types::INT_TYPE) {
        IntField f1 = (IntField &&) t1->getField(this->field1);
        IntField f2 = (IntField &&) t2->getField(this->field2);
        return f1.compare(this->op, &f2);
    }
    else if (t1->getField(field1).getType() == Types::STRING_TYPE && t2->getField(field2).getType() == Types::STRING_TYPE) {
        StringField f1 = (StringField &&) t1->getField(this->field1);
        StringField f2 = (StringField &&) t2->getField(this->field2);
        return f1.compare(this->op, &f2);
    }
    else {
        return false;
    }
}

int JoinPredicate::getField1() const {
    // TODO pa3.1: some code goes here
    return this->field1;
}

int JoinPredicate::getField2() const {
    // TODO pa3.1: some code goes here
    return this->field2;
}

Predicate::Op JoinPredicate::getOperator() const {
    // TODO pa3.1: some code goes here
    return this->op;
}
