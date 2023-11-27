#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2):
            p(p), child1(child1), child2(child2) {
    // TODO pa3.1: some code goes here
    this->children = {child1, child2};
    this->mergeTd = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
}

JoinPredicate *Join::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return this->p;
}

std::string Join::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return this->child1->getTupleDesc().getFieldName(this->p->getField1());
}

std::string Join::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return this->child2->getTupleDesc().getFieldName(this->p->getField2());
}

const TupleDesc &Join::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return mergeTd;
}

void Join::open() {
    // TODO pa3.1: some code goes here
    this->child1->open();
    this->child2->open();
    Operator::open();
}

void Join::close() {
    // TODO pa3.1: some code goes here
    this->child1->close();
    this->child2->close();
    Operator::close();
}

void Join::rewind() {
    // TODO pa3.1: some code goes here
    this->close();
    this->open();
}

std::vector<DbIterator *> Join::getChildren() {
    // TODO pa3.1: some code goes here
    return this->children;
}

void Join::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    this->children = children;
}

std::optional<Tuple> Join::fetchNext() {
    // TODO pa3.1: some code goes here
    while(true){
        switch(this->outer){
            case BEFORE:
                if(this->child1->hasNext()){
                    this->outerTuple=this->child1->next();
                    this->outer = READING;
                }
                else{
                    return std::nullopt;
                }
                break;

            case READING:
                if(this->inner == AFTER){
                    if(this->child1->hasNext()){
                        this->outerTuple=this->child1->next();
                        this->child2->rewind();
                        this->inner = BEFORE;
                    }
                    else{
                        this->outer = AFTER;
                        return std::nullopt;
                    }
                    continue;
                }
                break;

            case AFTER:
                return std::nullopt;

        }
        switch(this->inner){
            case BEFORE:
                if(this->child2->hasNext()){
                    this->innerTuple=this->child2->next();
                    this->inner = READING;
                }else{
                    return std::nullopt;
                }
                break;
            case READING:
                if(this->child2->hasNext()){
                    this->innerTuple=this->child2->next();
                }else{
                    this->inner = AFTER;
                    continue;
                }
                break;
            case AFTER:
                continue;

        }
        if(this->p->filter(&(outerTuple),&innerTuple)){
            Tuple mergeTuple = Tuple(this->mergeTd);
            for(int i=0; i<child1->getTupleDesc().numFields(); ++i){
                mergeTuple.setField(i, &outerTuple.getField(i));
            }
            for(int i=0; i<this->child2->getTupleDesc().numFields(); ++i){
                mergeTuple.setField(i + child1->getTupleDesc().numFields(), &innerTuple.getField(i));
            }
            return mergeTuple;
        }
    }
}
