/*
 * @Author       : Outsider
 * @Date         : 2023-08-26 17:02:33
 * @LastEditors  : Outsider
 * @LastEditTime : 2023-08-26 22:03:57
 * @Description  : In User Settings Edit
 * @FilePath     : /sigmod-2018/src/optimizer.cpp
 */
#include "optimizer.h"

void Optimizer::preparation(Joiner &joiner) {
    // Make statistical data for relations
    for (auto &relation: joiner.getRelations()) {
        for (auto &col: relation.columns()) {
            std::map<uint64_t, uint64_t> count;
            for (uint64_t i = 0; i < relation.size(); ++i) {
                count[col[i]]++;
            }
            auto minn = count.begin()->first;
            auto maxx = count.rbegin()->first;

            auto alike = std::max_element(count.begin(), count.end(), [](auto &lhs, auto &rhs) -> bool {
                return lhs.second < rhs.second;
            })->second;

            auto ndv = count.size();

//            auto maxx = *std::max_element(col, col + relation.size());
//            auto minn = *std::min_element(col, col + relation.size());
            relation.statistics.emplace_back(maxx, minn, ndv, alike, relation.size());
        }
//        for (uint64_t i = 0; i < relation.columns().size(); i++) {
//            relation.setMaxx(i, *std::max_element(relation.columns()[i], relation.columns()[i] + relation.size()));
//            relation.setMinn(i, *std::min_element(relation.columns()[i], relation.columns()[i] + relation.size()));
//        }
    }
}

void Optimizer::opt(Joiner &joiner, QueryInfo &queryInfo) {
    constantTransfer(joiner, queryInfo);

    optFilter(joiner, queryInfo);

    if (queryInfo.null_) {
        return;
    }

    estimateCost(joiner, queryInfo);
}

void Optimizer::optFilter(Joiner &joiner, QueryInfo &queryInfo) {
    for (auto &filter: queryInfo.filters()) {
        queryInfo.select_filters[filter.filter_column].emplace_back(filter);
    }

    for (auto &[lhs, rhs]: queryInfo.select_filters) {
        auto greater = rhs.end();
        auto less = rhs.end();
        auto equal = rhs.end();
        if (!rhs.empty()) {
            auto it = rhs.begin();
            while (it != rhs.end()) {
                switch (it->comparison) {
                    case FilterInfo::Less:
                        if (less == rhs.end()) {
                            less = it;
                        } else {
                            if (it->constant < less->constant) {
                                less = it;
                            }
                        }
                        break;
                    case FilterInfo::Greater:
                        if (greater == rhs.end()) {
                            greater = it;
                        } else {
                            if (it->constant > greater->constant) {
                                greater = it;
                            }
                        }
                        break;
                    case FilterInfo::Equal:
                        if (equal == rhs.end()) {
                            equal = it;
                        } else {
                            if (equal->constant != it->constant) {
                                queryInfo.null_ = true;
                                return;
                            }
                        }

                        break;
                }
                it++;
            }
            auto &relation = joiner.getRelation(lhs.rel_id);
            if (equal != rhs.end()) {
                if (greater != rhs.end() && greater->constant >= equal->constant) {
                    queryInfo.null_ = true;
                    return;
                }
                if (less != rhs.end() && less->constant >= equal->constant) {
                    queryInfo.null_ = true;
                    return;
                }
                if (equal->constant > relation.statistics[lhs.col_id].maxx_ ||
                    equal->constant < relation.statistics[lhs.col_id].minn_) {
                    queryInfo.null_ = true;
                    return;
                }
                queryInfo.select_filters[lhs] = {*equal};
            } else {
                std::vector<FilterInfo> result;
                if (greater != rhs.end()) {
                    result.emplace_back(*greater);
                    if (less != rhs.end() && less->constant >= greater->constant) {
                        queryInfo.null_ = true;
                        return;
                    }
                    if (greater->constant >= relation.statistics[lhs.col_id].maxx_) {
                        queryInfo.null_ = true;
                        return;
                    }
                }
                if (less != rhs.end()) {
                    if (less->constant <= relation.statistics[lhs.col_id].minn_) {
                        queryInfo.null_ = true;
                        return;
                    }
                    result.emplace_back(*less);
                }
                queryInfo.select_filters[lhs] = result;
            }
        }
    }

    auto &filters = const_cast<std::vector<FilterInfo> &>( queryInfo.filters());
    filters.clear();
    for (auto &[lhs, rhs]: queryInfo.select_filters) {
        std::copy(rhs.begin(), rhs.end(), std::back_inserter(filters));
    }
}

void Optimizer::estimateCost(Joiner &joiner, QueryInfo &queryInfo) {
    filterCost(joiner, queryInfo);
    predicateCost(joiner, queryInfo);
}

void Optimizer::filterCost(Joiner &joiner, QueryInfo &queryInfo) {
    auto &select_filters = queryInfo.select_filters;

    for (auto &[select, filters]: select_filters) {
        auto bind = select.binding;
        auto rel_id = select.rel_id;
        auto col_id = select.col_id;
        auto &relation = joiner.getRelation(rel_id);

        uint64_t maxx = relation.statistics[col_id].maxx_;
        uint64_t minn = relation.statistics[col_id].minn_;
        uint64_t cost = 0;
        if (filters.size() == 1) {
            uint64_t constant = filters[0].constant;
            switch (filters[0].comparison) {
                case FilterInfo::Greater:
                    cost = static_cast<uint64_t >(static_cast<double >((maxx - constant)) *
                                                  (static_cast<double >(relation.size()) /
                                                   static_cast<double >((maxx - minn))));
                    break;
                case FilterInfo::Less:
                    cost = static_cast<uint64_t >( static_cast<uint64_t >((constant - minn)) *
                                                   (static_cast<uint64_t >(relation.size()) /
                                                    static_cast<uint64_t >((maxx - minn))));
                    break;
                case FilterInfo::Equal:
                    cost = relation.statistics[col_id].alike_;
                    break;
            }
        } else {
            auto &lhs = filters[0];
            auto &rhs = filters[1];
            uint64_t left = std::min(lhs.constant, rhs.constant);
            uint64_t right = std::max(lhs.constant, rhs.constant);
            left = std::max(minn, left);
            right = std::min(maxx, right);
            cost = static_cast<uint64_t>( static_cast<double >((right - left)) *
                                          (static_cast<double >(relation.size()) /
                                           static_cast<double >((maxx - minn))));
        }
        if (queryInfo.relation_cost.find(bind) == queryInfo.relation_cost.end()) {
            queryInfo.relation_cost[bind] = std::min(cost, relation.size());
        } else {
            queryInfo.relation_cost[bind] = std::min(cost, queryInfo.relation_cost[bind]);
        }
    }
}

void Optimizer::predicateCost(Joiner &joiner, QueryInfo &queryInfo) {
    if (queryInfo.predicates().empty()) {
        return;
    }
    auto &predicates = const_cast<std::vector<PredicateInfo> &>(queryInfo.predicates());

    if (predicates.size() == 1) {
        return;
    }

    for (auto &predicate: predicates) {
        auto &lhs = predicate.left;
        auto &rhs = predicate.right;

        auto lhs_cost = joiner.getRelation(lhs.rel_id).size();
        auto rhs_cost = joiner.getRelation(rhs.rel_id).size();
        if (lhs.binding == rhs.binding) {
            if (queryInfo.relation_cost.find(lhs.binding) != queryInfo.relation_cost.end()) {
                lhs_cost = queryInfo.relation_cost[lhs.binding];
            }
            predicate.estimated_cost_ = static_cast<uint64_t >(sqrt(static_cast<double>(lhs_cost)));
        } else {
            if (queryInfo.relation_cost.find(lhs.binding) != queryInfo.relation_cost.end()) {
                lhs_cost = queryInfo.relation_cost[lhs.binding];
            }
            if (queryInfo.relation_cost.find(rhs.binding) != queryInfo.relation_cost.end()) {
                rhs_cost = queryInfo.relation_cost[rhs.binding];
            }
            predicate.estimated_cost_ = std::min(lhs_cost, rhs_cost);
        }
    }

    std::sort(predicates.begin(), predicates.end(), [](auto &lhs, auto &rhs) -> bool {
        return lhs.estimated_cost_ < rhs.estimated_cost_;
    });
}

void Optimizer::constantTransfer(Joiner &joiner, QueryInfo &queryInfo) {
    std::multimap<SelectInfo, SelectInfo> store;
    for (auto &predicate: queryInfo.predicates()) {
        store.insert({predicate.left, predicate.right});
        store.insert({predicate.right, predicate.left});
    }
    auto filters = queryInfo.filters();

    auto &qfilters = const_cast<std::vector<FilterInfo> &>(queryInfo.filters());

    for (auto &filter: filters) {
        std::map<SelectInfo, bool> vis;
        std::function<void(SelectInfo &selectInfo)> recursive = [&](SelectInfo &selectInfo) -> void {
            vis[selectInfo] = true;
            auto iter = store.equal_range(selectInfo);
            for (auto it = iter.first; it != iter.second; ++it) {
                if (!vis[it->second]) {
                    qfilters.emplace_back(it->second, filter.constant, filter.comparison);
                    recursive(it->second);
                }
            }
        };
        recursive(filter.filter_column);
    }
}

void Optimizer::clear() {
}