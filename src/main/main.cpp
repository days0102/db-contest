/*
 * @Author       : Outsider
 * @Date         : 2023-08-06 16:29:02
 * @LastEditors  : Outsider
 * @LastEditTime : 2023-08-26 21:51:54
 * @Description  : In User Settings Edit
 * @FilePath     : /sigmod-2018/src/main/main.cpp
 */
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <thread>

#include "joiner.h"
#include "parser.h"
#include "optimizer.h"

Joiner joiner;

int main(int argc, char *argv[]) {
    std::ios::sync_with_stdio(false);
    std::cin.tie();
    std::cout.tie();

    // Read join relations
    std::string line;
    while (getline(std::cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    // Preparation phase (not timed)
    // Build histograms, indexes,...
    std::unordered_map<std::string, std::string> cache;

    // Make statistical data
    Optimizer::preparation(joiner);

//    while (getline(std::cin, line)) {
//        if (line == "F") continue; // End of a batch
//        if (cache.find(line) != cache.end()) {
//            std::cout << cache[line];
//            continue;
//        }
//        QueryInfo i;
//        i.parseQuery(line);
//
//        Optimizer::opt(joiner, i);
//
//        std::string res = joiner.join(i);
//        cache[line] = res;
//        std::cout << res;
//    }

    std::vector<std::string> lines;
    while (getline(std::cin, line)) {
        if (line == "F") {
            uint64_t idx = 0;
            std::mutex latch_;
            std::vector<std::thread> threads;
            std::vector<std::string> result(lines.size());
            
            auto nt = std::thread::hardware_concurrency();
            for (uint64_t ix = 0; ix < 4; ix++) {
                threads.emplace_back([&] {
                    for (;;) {
                        latch_.lock();
                        if (idx >= lines.size()) {
                            latch_.unlock();
                            return;
                        }
                        uint64_t index = idx;
                        std::string s = lines[idx++];
                        latch_.unlock();
                        QueryInfo i;

                        i.parseQuery(s);

                        Optimizer::opt(joiner, i);

                        result[index] = joiner.join(i);
                    }
                });
            }
            for (auto &t: threads) {
                t.join();
            }
            for (auto &s: result) {
                std::cout << s;
            }
            lines.clear();
        } else {
            lines.push_back(line);
        }
    }

    return 0;
}
