/*
 * @Author       : Outsider
 * @Date         : 2023-08-26 16:59:18
 * @LastEditors  : Outsider
 * @LastEditTime : 2023-08-26 21:58:08
 * @Description  : In User Settings Edit
 * @FilePath     : /sigmod-2018/src/include/optimizer.h
 */
#pragma once

#include <map>
#include <cmath>

#include "parser.h"
#include "operators.h"
#include "joiner.h"
#include "relation.h"

class Optimizer {
private:

public:
    Optimizer() {}

    ~Optimizer() {}

    static void preparation(Joiner &);

    static void opt(Joiner &, QueryInfo &);

    static void optFilter(Joiner &, QueryInfo &);

    static void estimateCost(Joiner &, QueryInfo &);

    static void filterCost(Joiner &, QueryInfo &);

    static void predicateCost(Joiner &, QueryInfo &);

    static void constantTransfer(Joiner &, QueryInfo &);

    void clear();
};

