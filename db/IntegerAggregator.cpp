#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
    // TODO pa3.2: some code goes here
    int m_groupByFieldIndex;
    TupleDesc m_tupleDesc;
    const IntegerAggregator &m_aggregator;
    std::unordered_map<Field*, int>::const_iterator m_current;

public:
    IntegerAggregatorIterator(int gbfield,
                              const TupleDesc &td,
                              const IntegerAggregator &aggregator) :
                              m_groupByFieldIndex(gbfield), m_tupleDesc(td), m_aggregator(aggregator) {
        // TODO pa3.2: some code goes here
    }

    void open() override {
        // TODO pa3.2: some code goes here
        m_current = m_aggregator.m_aggregateData.begin();
    }

    bool hasNext() override {
        // TODO pa3.2: some code goes here
        return m_current != m_aggregator.m_aggregateData.end();
    }

    Tuple next() override {
        // TODO pa3.2: some code goes here
        if (!hasNext()) {
            throw std::runtime_error("No more elements");
        }

        Tuple tuple(m_tupleDesc);
        Field* groupField = m_current->first;
        int aggregateVal = m_current->second;

        db::IntegerAggregator::Op op = m_aggregator.m_op;  // Get the operation type from the aggregator
        if (op == db::Aggregator::Op::AVG && m_aggregator.m_count.find(groupField) != m_aggregator.m_count.end()) {
            aggregateVal /= m_aggregator.m_count.at(groupField);
        }

        if (m_groupByFieldIndex == Aggregator::NO_GROUPING) {
            tuple.setField(0, new IntField(aggregateVal));
        } else {
            tuple.setField(0, groupField);
            tuple.setField(1, new IntField(aggregateVal));
        }

        ++m_current;
        return tuple;
    }

    void rewind() override {
        // TODO pa3.2: some code goes here
        m_current = m_aggregator.m_aggregateData.begin();
    }

    const TupleDesc &getTupleDesc() const override {
        // TODO pa3.2: some code goes here
        return m_tupleDesc;
    }

    void close() override {
        // TODO pa3.2: some code goes here
        m_current = m_aggregator.m_aggregateData.end();
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Op what):
        m_groupByFieldIndex(gbfield), m_groupByFieldType(gbfieldtype), m_aggregateFieldIndex(afield), m_op(what){
    // TODO pa3.2: some code goes here
    m_aggregateData = std::unordered_map<Field*, int>();
    m_count = std::unordered_map<Field*, int>();
}

int IntegerAggregator::initialData() {
    switch (m_op) {
        case Op::MIN: return INT_MAX;
        case Op::MAX: return INT_MIN;
        case Op::SUM:
        case Op::COUNT:
        case Op::AVG: return 0;
        default: return 0;
    }
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // TODO pa3.2: some code goes here
    IntField* tupleGroupByField;
    if (m_groupByFieldIndex != NO_GROUPING) {
        *tupleGroupByField = (IntField &&) tup->getField(m_groupByFieldIndex);
    }
    if (m_aggregateData.find(tupleGroupByField) == m_aggregateData.end()) {
        m_aggregateData[tupleGroupByField] = initialData();
        m_count[tupleGroupByField] = 0;
    }

    int tupleValue = tup->getField(m_aggregateFieldIndex).getType();
    int currentValue = m_aggregateData[tupleGroupByField];
    int currentCount = m_count[tupleGroupByField];
    int newValue = currentValue;

    switch (m_op) {
        case Op::MIN:
            newValue = std::min(currentValue, tupleValue);
            break;
        case Op::MAX:
            newValue = std::max(currentValue, tupleValue);
            break;
        case Op::SUM:
        case Op::AVG:
            m_count[tupleGroupByField] = currentCount + 1;
            newValue = tupleValue + currentValue;
            break;
        case Op::COUNT:
            newValue = currentValue + 1;
            break;
        default:
            break; // should not reach here
    }

    m_aggregateData[tupleGroupByField] = newValue;
}

DbIterator *IntegerAggregator::iterator() const {
    // TODO pa3.2: some code goes here
}