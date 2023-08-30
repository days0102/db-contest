/*
 * @Author       : Outsider
 * @Date         : 2023-08-06 16:29:02
 * @LastEditors  : Outsider
 * @LastEditTime : 2023-08-26 21:48:18
 * @Description  : In User Settings Edit
 * @FilePath     : /sigmod-2018/src/include/relation.h
 */
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "statistic.h"

using RelationId = unsigned;

class Relation {
private:
    /// Owns memory (false if it was mmaped)
    bool owns_memory_;
    /// The number of tuples
    uint64_t size_;
    /// The join column containing the keys
    std::vector<uint64_t *> columns_;

    // Data characteristics

    std::vector<uint64_t> maxx_;
    std::vector<uint64_t> minn_;

public:
    /// Constructor without mmap
    Relation(uint64_t size, std::vector<uint64_t *> &&columns)
            : owns_memory_(true), size_(size), columns_(columns) {
        maxx_.resize(columns.size());
        minn_.resize(columns.size());
    }

    /// Constructor using mmap
    explicit Relation(const char *file_name);

    /// Delete copy constructor
    Relation(const Relation &other) = delete;

    /// Move constructor
    Relation(Relation &&other) = default;

    /// The destructor
    ~Relation();

    /// Stores a relation into a file (binary)
    void storeRelation(const std::string &file_name);

    /// Stores a relation into a file (csv)
    void storeRelationCSV(const std::string &file_name);

    /// Dump SQL: Create and load table (PostgreSQL)
    void dumpSQL(const std::string &file_name, unsigned relation_id);

    /// The number of tuples
    uint64_t size() const {
        return size_;
    }

    /// The join column containing the keys
    const std::vector<uint64_t *> &columns() const {
        return columns_;
    }

    std::vector<ColumnStatistics> statistics;

    void setMaxx(uint64_t, uint64_t);

    void setMinn(uint64_t, uint64_t);

    const uint64_t getMaxx(uint64_t col_id) const { return maxx_[col_id]; }

    const uint64_t getMinn(uint64_t col_id) const { return minn_[col_id]; }

private:
    /// Loads data from a file
    void loadRelation(const char *file_name);
};

