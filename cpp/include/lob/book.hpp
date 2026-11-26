// book.hpp: L2 order book with integer prices and quantities (header-only).
//
// Same semantics as book.py, the Python reference: a depth update gives the NEW total quantity of a
// price level, and quantity 0 removes the level. Prices are integer ticks (BTCUSDT: 0.01 USDT) and
// quantities integer lots (1e-8 BTC). They are converted once when the data are loaded, so every
// comparison is exact and cheap.
//
// A book side keeps its levels ordered from the worst price to the best one, so the best level is
// always the LAST element. Two interchangeable containers for a side:
//   VecSide: a sorted std::vector. Contiguous memory; a level is found by binary search; inserting or
//            erasing shifts every element after it, i.e. few elements near the best price and many
//            deep in the book.
//   MapSide: a std::map (red-black tree). O(log n) for every operation, one heap node per level.
// Which one is faster on real Binance data is measured by the benchmark, not assumed.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <vector>

namespace lob {

using Price = std::int64_t;  // integer ticks
using Qty = std::int64_t;    // integer lots

enum Side : int { kBid = 0, kAsk = 1 };  // same encoding as the 'side' column of the Parquet tables

struct Level {
    Price price;
    Qty qty;
};

inline bool operator==(const Level& a, const Level& b) { return a.price == b.price && a.qty == b.qty; }

// The template parameter Worse(a, b) is true when price a is worse than price b on this side:
//   bids: std::less<Price>    (a lower bid is worse)  -> ascending order, best (highest) bid last
//   asks: std::greater<Price> (a higher ask is worse) -> descending order, best (lowest) ask last

template <class Worse>
class VecSide {
public:
    // Set the quantity of a price level; q == 0 removes it. Returns false only when asked to remove a
    // level that is not there (normal on Binance: deeper than our snapshot, or already removed).
    bool set(Price p, Qty q) {
        // first level whose price is NOT worse than p, i.e. equal to p or better
        auto it = std::lower_bound(levels_.begin(), levels_.end(), p,
                                   [](const Level& l, Price x) { return Worse{}(l.price, x); });
        const bool found = it != levels_.end() && it->price == p;
        if (q == 0) {
            if (!found) return false;
            levels_.erase(it);       // shifts the better levels one step to the left
        } else if (found) {
            it->qty = q;             // absolute quantity: replace, do not add
        } else {
            levels_.insert(it, Level{p, q});  // shifts the better levels one step to the right
        }
        return true;
    }

    // Replace every level at once (snapshot). Sorting once is O(n log n); inserting the levels one by
    // one could be O(n^2) because each insert may shift the whole vector. Levels with q == 0 are skipped.
    void assign(std::vector<Level> levels) {
        levels.erase(std::remove_if(levels.begin(), levels.end(), [](const Level& l) { return l.qty == 0; }),
                     levels.end());
        std::sort(levels.begin(), levels.end(),
                  [](const Level& a, const Level& b) { return Worse{}(a.price, b.price); });
        levels_ = std::move(levels);
    }

    std::optional<Level> best() const {
        if (levels_.empty()) return std::nullopt;
        return levels_.back();
    }

    // Append the n best levels to out, best first.
    void top(std::size_t n, std::vector<Level>& out) const {
        for (auto it = levels_.rbegin(); it != levels_.rend() && n > 0; ++it, --n) out.push_back(*it);
    }

    // Call f(level) for each level from the best one on; stop as soon as f returns false.
    template <class F>
    void for_each_best_first(F f) const {
        for (auto it = levels_.rbegin(); it != levels_.rend(); ++it)
            if (!f(*it)) return;
    }

    std::size_t size() const { return levels_.size(); }
    void clear() { levels_.clear(); }

private:
    std::vector<Level> levels_;  // ordered from worst to best
};

template <class Worse>
class MapSide {
public:
    bool set(Price p, Qty q) {
        if (q == 0) return levels_.erase(p) == 1;  // erase returns how many keys were removed
        levels_[p] = q;
        return true;
    }

    void assign(const std::vector<Level>& levels) {
        levels_.clear();
        for (const Level& l : levels)
            if (l.qty != 0) levels_[l.price] = l.qty;
    }

    std::optional<Level> best() const {
        if (levels_.empty()) return std::nullopt;
        const auto& [p, q] = *levels_.rbegin();  // the map is ordered from worst to best
        return Level{p, q};
    }

    void top(std::size_t n, std::vector<Level>& out) const {
        for (auto it = levels_.rbegin(); it != levels_.rend() && n > 0; ++it, --n)
            out.push_back(Level{it->first, it->second});
    }

    template <class F>
    void for_each_best_first(F f) const {
        for (auto it = levels_.rbegin(); it != levels_.rend(); ++it)
            if (!f(Level{it->first, it->second})) return;
    }

    std::size_t size() const { return levels_.size(); }
    void clear() { levels_.clear(); }

private:
    std::map<Price, Qty, Worse> levels_;  // ordered from worst to best
};

// A book = one side container per side + the update id the book is consistent with.
// SideT is VecSide or MapSide; both have the same interface.
template <template <class> class SideT>
class BasicBook {
public:
    SideT<std::less<Price>> bids;
    SideT<std::greater<Price>> asks;
    std::int64_t last_update_id = -1;  // -1: no state yet

    void clear() {
        bids.clear();
        asks.clear();
        last_update_id = -1;
    }

    void load_snapshot(std::int64_t last_id, std::vector<Level> bid_levels, std::vector<Level> ask_levels) {
        bids.assign(std::move(bid_levels));
        asks.assign(std::move(ask_levels));
        last_update_id = last_id;
    }

    // Set one level. Returns false for the removal of a missing level.
    bool set(Side side, Price p, Qty q) { return side == kBid ? bids.set(p, q) : asks.set(p, q); }

    // Apply the n levels of one depth event (sides may be mixed). Sequence checks (gaps, stale events)
    // belong to the replay engine, as in book.py. Returns the number of removals of missing levels.
    std::int64_t apply(const std::int8_t* side, const Price* price, const Qty* qty, std::size_t n,
                       std::int64_t last_id) {
        std::int64_t missing = 0;
        for (std::size_t i = 0; i < n; ++i)
            if (!set(static_cast<Side>(side[i]), price[i], qty[i])) ++missing;
        last_update_id = last_id;
        return missing;
    }

    bool crossed() const {
        const std::optional<Level> b = bids.best();
        const std::optional<Level> a = asks.best();
        return b && a && b->price >= a->price;
    }
};

using VecBook = BasicBook<VecSide>;
using MapBook = BasicBook<MapSide>;

}  // namespace lob
