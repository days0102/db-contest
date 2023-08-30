//
// Created by LWZ on 2023/8/29.
//
#include "statistic.h"

uint64_t ColumnStatistics::GetGreaterCount(uint64_t constant) {
    auto it = histograms.lower_bound(constant);
    uint64_t res(0);
    for (; it != histograms.end(); ++it) {
        res += it->second;
    }
    return res;
}

uint64_t ColumnStatistics::GetLessCount(uint64_t constant) {
    auto it = histograms.upper_bound(constant);
    uint64_t res(0);
    for (auto iter = histograms.begin(); iter != it; ++iter) {
        res += iter->second;
    }
    return res;
}