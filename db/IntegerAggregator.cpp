#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

IntAggregationHelper::IntAggregationHelper() : sum(0), minVal(0), maxVal(0), numOfElements(0) {}

int IntAggregationHelper::getMinimum() const { return minVal; }
int IntAggregationHelper::getMaximum() const { return maxVal; }
int IntAggregationHelper::getSum() const { return sum; }
int IntAggregationHelper::getCount() const { return numOfElements; }

void IntAggregationHelper::addElement(int value) {
    elements.push_back(value);
    sum += value;
    minVal = std::min(minVal, value);
    maxVal = std::max(maxVal, value);
    numOfElements++;
}

class IntegerAggregatorIterator : public DbIterator {
    // TODO pa3.2: some code goes here
    int groupByField;
    std::unordered_map<int, IntAggregationHelper> aggregationData;
    std::unordered_map<int, IntAggregationHelper>::iterator iter;
    TupleDesc tupleDescription;
    Aggregator::Op operation;

public:
    IntegerAggregatorIterator(int gbField, const std::unordered_map<int, IntAggregationHelper> &data, Aggregator::Op op) :
                              groupByField(gbField), aggregationData(data), operation(op), tupleDescription(*new TupleDesc({Types::INT_TYPE})){
        // TODO pa3.2: some code goes here
    }

    void open() override {
        // TODO pa3.2: some code goes here
        iter = aggregationData.begin();
    }

    bool hasNext() override {
        // TODO pa3.2: some code goes here
        return iter != aggregationData.end();
    }

    Tuple next() override {
        // TODO pa3.2: some code goes here
        auto current = *iter;
        IntAggregationHelper& aggData = current.second;
        int resultValue;
        switch (operation) {
            case Aggregator::Op::COUNT:
                resultValue = aggData.getCount();
                break;
            case Aggregator::Op::MIN:
                resultValue = aggData.getMinimum();
                break;
            case Aggregator::Op::MAX:
                resultValue = aggData.getMaximum();
                break;
            case Aggregator::Op::AVG:
                resultValue = aggData.getCount() > 0 ? aggData.getSum() / aggData.getCount() : 0;
                break;
            case Aggregator::Op::SUM:
                resultValue = aggData.getSum();
                break;
            default: break;
        }

        Tuple resultTuple = Tuple(tupleDescription);
        if (groupByField == Aggregator::NO_GROUPING) {
            IntField field = IntField(resultValue);
            resultTuple.setField(0, &field);
        } else {
            IntField groupField = IntField(current.first);
            IntField resultField = IntField(resultValue);
            resultTuple.setField(0, &groupField);
            resultTuple.setField(1, &resultField);
        }
        ++iter;
        return resultTuple;
    }

    void rewind() override {
        // TODO pa3.2: some code goes here
        close();
        open();
    }

    const TupleDesc &getTupleDesc() const override {
        // TODO pa3.2: some code goes here
        return tupleDescription;
    }

    void close() override {
        // TODO pa3.2: some code goes here
        aggregationData.clear();
        iter = aggregationData.end();
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Op what):
        groupByField(gbfield), groupByFieldType(gbfieldtype), aggregateField(afield), operation(what) {
    // TODO pa3.2: some code goes here
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // TODO pa3.2: some code goes here
    IntField aggField = (IntField&&)(tup->getField(this->aggregateField));

    int aggValue = aggField.getValue();

    // Determine the key for grouping
    int groupKey;
    if (this->groupByField == Aggregator::NO_GROUPING) {
        groupKey = 0;
    }
    else {
        IntField temp = (IntField &&) tup->getField(this->groupByField);
        groupKey = temp.getValue();
    }

    // Initialize group data if not present
    if (this->aggregationMap.find(groupKey) == this->aggregationMap.end()) {
        this->aggregationMap[groupKey] = IntAggregationHelper();
    }

    // Update the aggregate data for the group
    this->aggregationMap[groupKey].addElement(aggValue);
}

DbIterator *IntegerAggregator::iterator() const {
    // TODO pa3.2: some code goes here
    return new IntegerAggregatorIterator(this->groupByField, this->aggregationMap, this->operation);
}