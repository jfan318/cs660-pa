#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    // TODO pa3.2: some code goes here
    int gbfield;
    const TupleDesc td;
    std::unordered_map <Field *, int> count;
    std::unordered_map <int, IntegerAggregatorHelper> aggMap;
    std::unordered_map <int, IntegerAggregatorHelper>::iterator aggMapIter;
    TupleDesc resultTD;
    Aggregator::Op what;

public:
    IntegerAggregatorIterator(int gbfield,
                              const std::unordered_map<int, IntegerAggregatorHelper> &data,
                              Aggregator::Op what)
            :aggMap(data), gbfield(gbfield), what(what){
        // TODO pa3.2: some code goes here
        this->resultTD = *new TupleDesc({Types::INT_TYPE});
    }

    void open() override {
        // TODO pa3.2: some code goes here
        this->aggMapIter = this->aggMap.begin();
    }

    bool hasNext() override {
        // TODO pa3.2: some code goes here
        this->aggMapIter = this->aggMap.end();
    }

    Tuple next() override {
        // TODO pa3.2: some code goes here
        if (aggMapIter == aggMap.end()) {
            return {};
        }

        auto &currentEntry = *aggMapIter;
        IntegerAggregatorHelper& helper = currentEntry.second;
        Tuple resultTuple = Tuple(resultTD);
        int aggregateResult;

        switch (what) {
            case Aggregator::Op::SUM:
                aggregateResult = helper.getSum();
                break;
            case Aggregator::Op::MIN:
                aggregateResult = helper.getMin();
                break;
            case Aggregator::Op::MAX:
                aggregateResult = helper.getMax();
                break;
            case Aggregator::Op::COUNT:
                aggregateResult = helper.getCount();
                break;
            default:
                throw std::runtime_error("Unsupported aggregation operation");
        }

        if (this->gbfield == Aggregator::NO_GROUPING) {
            resultTuple.setField(0, reinterpret_cast<const Field *>(aggregateResult));
        } else {
            resultTuple.setField(0, reinterpret_cast<const Field *>(aggregateResult));
            resultTuple.setField(1, reinterpret_cast<const Field *>(aggregateResult));
        }

        ++aggMapIter;
        return resultTuple;
    }

    void rewind() override {
        // TODO pa3.2: some code goes here
        this->close();
        this->open();
    }

    const TupleDesc &getTupleDesc() const override {
        // TODO pa3.2: some code goes here
        return this->td;
    }

    void close() override {
        // TODO pa3.2: some code goes here
        this->aggMapIter = this->aggMap.end();
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what):
                                     gbfield(gbfield), gbfieldtype(gbfieldtype), afield(afield), what(what){
    // TODO pa3.2: some code goes here
    if (gbfield == NO_GROUPING) {
        aggMap[0] = IntegerAggregatorHelper();
    }
    std::vector<Types::Type> fieldTypes;
    std::vector<std::string> fieldNames;

    if (gbfield == NO_GROUPING) {
        fieldTypes.push_back(Types::INT_TYPE);
        fieldNames.push_back("aggregateValue");
        fieldTypes.push_back(gbfieldtype.value());
        fieldNames.push_back("groupByField");
        fieldTypes.push_back(Types::INT_TYPE);
        fieldNames.push_back("aggregateValue");
    }
    this->td = TupleDesc(fieldTypes, fieldNames);
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // TODO pa3.2: some code goes here
    IntField aggField = (IntField &&) tup->getField(this->afield);

    if (gbfield == NO_GROUPING) {
        aggMap[0].addKey(aggField.getValue());
    } else {
        IntField field = (IntField &&) tup->getField(this->gbfield);
        int key = field.getValue();
        groupCountMap[key]++;

        if (aggMap.find(key) == aggMap.end()) {
            aggMap[key] = IntegerAggregatorHelper();
        }
        aggMap[key].addKey(aggField.getValue());
    }
    invalid = true;
}

DbIterator *IntegerAggregator::iterator() const {
    // TODO pa3.2: some code goes here
    return new IntegerAggregatorIterator(this->gbfield,this->aggMap, this->what);
}
