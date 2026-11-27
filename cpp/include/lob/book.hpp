// book.hpp: L2 order book with integer prices and quantities (header-only).
//
// Same semantics as book.py, the Python reference: a depth update gives the NEW total quantity of a
// price level, and quantity 0 removes the level. Prices are integer ticks (BTCUSDT: 0.01 USDT) and
// quantities integer lots (1e-8 BTC). They are converted once when the data are loaded, so every
// comparison is exact and cheap.
//
// Three interchangeable containers for one side of the book, with the same interface:
//   VecSide:    a sorted std::vector, ordered from the worst price to the best (best = last element).
//               Contiguous memory; a level is found by binary search; inserting or erasing shifts every
//               element after it, i.e. few elements near the best price and many deep in the book.
//   MapSide:    a std::map (red-black tree), same order. O(log n) for every operation, one heap node
//               per level.
//   LadderSide: a price ladder: an array indexed by price over a window of 2^N ticks around the best
//               price, plus a std::map for the few levels below the window. Designed from the measured
//               update stream (see the comment above the class).
// Which one is faster on real Binance data is measured by the benchmark, not assumed.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <map>
#include <optional>
#include <type_traits>
#include <vector>

namespace lob {

using Price = std::int64_t;  // integer ticks
using Qty = std::int64_t;    // integer lots

enum Side : int { kBid = 0, kAsk = 1 };  // same encoding as the 'side' column of the Parquet tables

struct Level {
    Price price;
    Qty qty;
};

// Where a price sits in a side, without changing it (used by the benchmark's depth profile).
struct Probe {
    bool found;          // a level at this price exists
    std::size_t better;  // number of levels with a strictly better price
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

    // For an insert or erase at p, 'better' is also the number of elements the vector has to shift.
    Probe probe(Price p) const {
        auto it = std::lower_bound(levels_.begin(), levels_.end(), p,
                                   [](const Level& l, Price x) { return Worse{}(l.price, x); });
        const bool found = it != levels_.end() && it->price == p;
        return Probe{found, static_cast<std::size_t>(levels_.end() - it) - (found ? 1 : 0)};
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

    Probe probe(Price p) const {  // O(n) because of std::distance: for profiling only
        auto it = levels_.lower_bound(p);  // first price that is not worse than p
        const bool found = it != levels_.end() && it->first == p;
        return Probe{found, static_cast<std::size_t>(std::distance(it, levels_.end())) - (found ? 1 : 0)};
    }

    std::size_t size() const { return levels_.size(); }
    void clear() { levels_.clear(); }

private:
    std::map<Price, Qty, Worse> levels_;  // ordered from worst to best
};

// LadderSide: a price ladder, designed from the measured update stream (bench.py sections 5 and 6,
// 24 h of BTCUSDT):
//   * most level updates land within a few hundred USDT of the best price, i.e. a few 10,000 ticks:
//     there, an array indexed by price does an update with one memory access, without any search,
//     allocation or shifting;
//   * about 3 % land 5,000 to 21,000 USDT away: an array covering them would be huge and almost empty,
//     so the levels below the window stay in a std::map, as in MapSide;
//   * when the best level is erased, the next one is usually close (99.3 % within 255 ticks), so it is
//     found by scanning the array downwards; a bitmap of the occupied slots is not needed.
//
// Keys: on both sides a LARGER key is a BETTER price (bids: key = price, asks: key = -price), so the
// same code serves both sides. Array slot i holds the level whose key is base_ + i (qty 0 = no level).
// Invariants (valid() checks them for the tests):
//   1. every level with a key >= base_ is in the array, every level with a key < base_ is in deep_;
//      no level has a key above the window (>= base_ + kSlots);
//   2. count_ = number of non-empty slots; top_ = index of the highest non-empty slot, -1 if none;
//   3. a side that is not empty has its best level in the upper half of the window (top_ >= kLowSlot).
// The window moves when an insert lands above it, or when erases leave the best level below the
// middle of the window (or in deep_). After a move the best level is on slot kBestSlot (3/4 of the
// window), so the window always holds at least kSlots / 2 ticks behind the best price and up to
// kSlots / 4 ticks in front of it. A move costs O(kSlots) but is rare: shifts_up() / shifts_down()
// count the moves, deep_sets() the updates that went to the std::map.
template <class Worse, int kLog2Slots>
class LadderSide {
    static_assert(std::is_same_v<Worse, std::less<Price>> || std::is_same_v<Worse, std::greater<Price>>,
                  "LadderSide supports the bid order (std::less) and the ask order (std::greater)");
    static_assert(kLog2Slots >= 2 && kLog2Slots <= 26, "a window of 4 to 2^26 slots");

public:
    static constexpr std::int64_t kSlots = std::int64_t{1} << kLog2Slots;
    static constexpr std::int64_t kBestSlot = kSlots - kSlots / 4 - 1;  // where a move puts the best level
    static constexpr std::int64_t kLowSlot = kSlots / 2;               // best below this: move the window down

    LadderSide() : qty_(static_cast<std::size_t>(kSlots), 0) {}  // on the heap: 512 KiB for 2^16 slots

    bool set(Price p, Qty q) {
        const Price k = key_of(p);
        if (q != 0 && empty()) base_ = k - kBestSlot;  // first level of an empty side: window around it
        if (k < base_) {                                // below the window: the std::map
            ++deep_sets_;
            if (q == 0) return deep_.erase(k) == 1;
            deep_[k] = q;
            return true;
        }
        if (k >= base_ + kSlots) {                      // above the window: better than every level
            if (q == 0) return false;                   // no level can be there (invariant 1)
            move_window(k);                             // k becomes the best level, on kBestSlot
        }
        const std::int64_t i = k - base_;
        Qty& slot = qty_[at(i)];
        if (q != 0) {
            if (slot == 0) {
                ++count_;
                if (i > top_) top_ = i;                 // a new best level
            }
            slot = q;                                   // absolute quantity: replace, do not add
            return true;
        }
        if (slot == 0) return false;                    // removal of a level that is not there
        slot = 0;
        --count_;
        if (i == top_) {                                // the best level is gone: scan down to the next one
            do {
                --top_;
            } while (top_ >= 0 && qty_[at(top_)] == 0);
            if (top_ < kLowSlot) recentre();
        }
        return true;
    }

    void assign(const std::vector<Level>& levels) {
        clear();
        bool any = false;
        Price best = 0;
        for (const Level& l : levels) {
            if (l.qty == 0) continue;
            const Price k = key_of(l.price);
            if (!any || k > best) best = k;
            any = true;
        }
        if (!any) return;
        base_ = best - kBestSlot;
        for (const Level& l : levels) {
            if (l.qty == 0) continue;
            const Price k = key_of(l.price);
            if (k < base_) {
                deep_[k] = l.qty;
                continue;
            }
            Qty& slot = qty_[at(k - base_)];
            if (slot == 0) ++count_;
            slot = l.qty;
        }
        top_ = kBestSlot;
    }

    std::optional<Level> best() const {
        if (top_ < 0) return std::nullopt;  // invariant 3: a side that is not empty has its best in the array
        return Level{price_of(base_ + top_), qty_[at(top_)]};
    }

    void top(std::size_t n, std::vector<Level>& out) const {
        if (n == 0) return;
        for_each_best_first([&](const Level& l) {
            out.push_back(l);
            return --n > 0;
        });
    }

    template <class F>
    void for_each_best_first(F f) const {
        for (std::int64_t i = top_; i >= 0; --i) {
            const Qty q = qty_[at(i)];
            if (q != 0 && !f(Level{price_of(base_ + i), q})) return;
        }
        for (auto it = deep_.rbegin(); it != deep_.rend(); ++it)  // deep_ is ordered worst first
            if (!f(Level{price_of(it->first), it->second})) return;
    }

    Probe probe(Price p) const {  // O(kSlots): for profiling only
        const Price k = key_of(p);
        if (k >= base_ + kSlots) return Probe{false, 0};  // better than every level
        if (k >= base_) {
            const std::int64_t i = k - base_;
            std::size_t better = 0;
            for (std::int64_t j = i + 1; j <= top_; ++j)
                if (qty_[at(j)] != 0) ++better;
            return Probe{qty_[at(i)] != 0, better};
        }
        const auto above = deep_.upper_bound(k);  // first deep level with a better price
        const bool found = above != deep_.begin() && std::prev(above)->first == k;
        return Probe{found, count_ + static_cast<std::size_t>(std::distance(above, deep_.end()))};
    }

    std::size_t size() const { return count_ + deep_.size(); }

    void clear() {
        std::fill(qty_.begin(), qty_.end(), Qty{0});
        deep_.clear();
        count_ = 0;
        top_ = -1;
    }

    // Counters for bench.py; never reset, not even by clear().
    std::int64_t shifts_up() const { return shifts_up_; }
    std::int64_t shifts_down() const { return shifts_down_; }
    std::int64_t deep_sets() const { return deep_sets_; }

    // Invariants 1-3 (for the tests; O(kSlots + number of levels)).
    bool valid() const {
        std::size_t n = 0;
        std::int64_t high = -1;
        for (std::int64_t i = 0; i < kSlots; ++i) {
            if (qty_[at(i)] != 0) {
                ++n;
                high = i;
            }
        }
        if (n != count_ || high != top_) return false;                        // 2
        if (!deep_.empty() && deep_.rbegin()->first >= base_) return false;  // 1 (the array is the window)
        for (const auto& kv : deep_)
            if (kv.second == 0) return false;                                // no empty level is stored
        return size() == 0 || top_ >= kLowSlot;                               // 3
    }

private:
    static constexpr bool kIsBid = std::is_same_v<Worse, std::less<Price>>;

    static Price key_of(Price p) {  // larger key = better price, on both sides
        if constexpr (kIsBid) return p;
        else return -p;
    }
    static Price price_of(Price k) {
        if constexpr (kIsBid) return k;
        else return -k;
    }
    static std::size_t at(std::int64_t i) { return static_cast<std::size_t>(i); }

    bool empty() const { return count_ == 0 && deep_.empty(); }

    // After an erase left the best level below the middle of the window, or the array empty: bring the
    // best level back to kBestSlot (invariant 3). Nothing to do if the side is now empty.
    void recentre() {
        if (top_ >= 0) move_window(base_ + top_);
        else if (!deep_.empty()) move_window(deep_.rbegin()->first);
    }

    // Move the window so that key k lands on slot kBestSlot. k is either a new level above the window
    // (move up) or the current best level (move down). Levels that leave the window at the bottom go to
    // deep_; levels of deep_ that enter it move into the array. Nothing can leave at the top: no level
    // is better than k, and k lands below the top of the window.
    void move_window(Price k) {
        const Price new_base = k - kBestSlot;
        if (new_base > base_) {
            const std::int64_t s = new_base - base_;        // every level moves s slots down
            const std::int64_t gone = std::min(s, kSlots);  // the bottom slots leave the window
            for (std::int64_t i = 0; i < gone; ++i) {
                if (qty_[at(i)] != 0) {
                    // the keys increase and exceed every key already in deep_: insert at the end, O(1)
                    deep_.emplace_hint(deep_.end(), base_ + i, qty_[at(i)]);
                    --count_;
                }
            }
            std::copy(qty_.begin() + gone, qty_.end(), qty_.begin());
            std::fill(qty_.end() - gone, qty_.end(), Qty{0});
            top_ = top_ >= s ? top_ - s : -1;
            ++shifts_up_;
        } else if (new_base < base_) {
            const std::int64_t s = base_ - new_base;                        // every level moves s slots up
            const std::int64_t kept = std::max<std::int64_t>(kSlots - s, 0);  // the top s slots are empty
            std::copy_backward(qty_.begin(), qty_.begin() + kept, qty_.end());
            std::fill(qty_.begin(), qty_.begin() + (kSlots - kept), Qty{0});
            for (auto it = deep_.lower_bound(new_base); it != deep_.end(); it = deep_.erase(it)) {
                qty_[at(it->first - new_base)] = it->second;
                ++count_;
            }
            top_ = kBestSlot;  // k was the best level
            ++shifts_down_;
        }
        base_ = new_base;
    }

    std::vector<Qty> qty_;        // kSlots slots; slot i: quantity of the level with key base_ + i (0: none)
    Price base_ = 0;              // key of slot 0
    std::int64_t top_ = -1;       // highest non-empty slot, -1 if the array is empty
    std::size_t count_ = 0;       // non-empty slots
    std::map<Price, Qty> deep_;   // levels below the window, by key (ascending: worst first)
    std::int64_t shifts_up_ = 0, shifts_down_ = 0, deep_sets_ = 0;
};

// BasicBook takes a side template with one parameter (the price order); Ladder<N>::Side is LadderSide
// with its window fixed to 2^N ticks.
template <int kLog2Slots>
struct Ladder {
    template <class Worse>
    using Side = LadderSide<Worse, kLog2Slots>;
};

// A book = one side container per side + the update id the book is consistent with.
// SideT is VecSide, MapSide or Ladder<N>::Side; all have the same interface.
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
template <int kLog2Slots>
using LadderBookOf = BasicBook<Ladder<kLog2Slots>::template Side>;
// 2^16 ticks = 655.36 USDT: the smallest window past the knee of the measured distance distribution
// (bench.py section 5); bench.py also measures 2^12, 2^14 and 2^18.
using LadderBook = LadderBookOf<16>;

}  // namespace lob
