#include <db/JoinOptimizer.h>
#include <db/PlanCache.h>
#include <cfloat>

using namespace db;

double JoinOptimizer::estimateJoinCost(int card1, int card2, double cost1, double cost2) {
    // TODO pa4.2: some code goes here
    return cost1 + card1 * cost2 + card1 * card2;
}

int JoinOptimizer::estimateTableJoinCardinality(Predicate::Op joinOp,
                                                const std::string &table1Alias, const std::string &table2Alias,
                                                const std::string &field1PureName, const std::string &field2PureName,
                                                int card1, int card2, bool t1pkey, bool t2pkey,
                                                const std::unordered_map<std::string, TableStats> &stats,
                                                const std::unordered_map<std::string, int> &tableAliasToId) {
    // TODO pa4.2: some code goes here
    int result;

    if (joinOp == Predicate::Op::EQUALS) {
        if (t1pkey && !t2pkey) {
            result = card2;
        }
        else if (t2pkey && !t1pkey) {
            result = card1;
        }
        else if (t1pkey && t2pkey) {
            if (card1 >= card2) {result = card2;}
            else {result = card1;}
        }
        else {
            if (card1 >= card2) {result = card1;}
            else {result = card2;}
        }
    } else {
        result = static_cast<int>(0.3 * card1 * card2);
    }
    return result;
}

std::vector<LogicalJoinNode> JoinOptimizer::orderJoins(std::unordered_map<std::string, TableStats> stats,
                                                       std::unordered_map<std::string, double> filterSelectivities) {
    // TODO pa4.3: some code goes here

}
