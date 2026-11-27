// test_book.cpp: C++ unit tests for book.hpp, without an external test framework.
//
// Every test runs on every container (VecBook, MapBook, and the price ladder with its real window of
// 2^18 slots and with tiny windows of 4 or 8 slots, where the window moves at almost every update).
// The randomized tests compare them with a deliberately naive implementation (unsorted vector, linear
// search, sort on every query), so a bug shared by the real containers is unlikely to go unnoticed.
// The ladder also gets its own tests: a random walk with large jumps that moves the window often in
// both directions (with its invariants checked after every update), and hand-computed edge cases.
//
// Build and run (PowerShell, from the repo root):
//     cmake --build build --config Release
//     .\build\Release\lob_tests.exe
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <random>
#include <vector>

#include "lob/book.hpp"

using lob::kAsk;
using lob::kBid;
using lob::Level;
using lob::Price;
using lob::Qty;

static int g_checks = 0;
static int g_failed = 0;

// Count a check; on failure print where it failed and go on (so one run shows every failure).
#define CHECK(cond)                                                              \
    do {                                                                         \
        ++g_checks;                                                              \
        if (!(cond)) {                                                           \
            ++g_failed;                                                          \
            std::printf("%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                        \
    } while (0)

template <class Side>
static std::vector<Level> all_levels(const Side& s) {
    std::vector<Level> out;
    s.top(s.size(), out);
    return out;
}

template <class Book>
static void test_basic() {
    Book b;
    b.load_snapshot(10, {{99, 1}, {97, 3}, {98, 2}, {96, 0}}, {{101, 5}, {102, 6}});
    CHECK(b.last_update_id == 10);
    CHECK(b.bids.size() == 3);                                   // the qty-0 level was skipped
    CHECK(b.bids.best() == std::optional<Level>(Level{99, 1}));  // highest bid
    CHECK(b.asks.best() == std::optional<Level>(Level{101, 5})); // lowest ask
    CHECK((all_levels(b.bids) == std::vector<Level>{{99, 1}, {98, 2}, {97, 3}}));  // best first

    CHECK(b.set(kBid, 99, 4));                                   // absolute: 4, not 1 + 4
    CHECK(b.bids.best()->qty == 4);
    CHECK(b.set(kBid, 98, 0));                                   // remove
    CHECK(!b.set(kBid, 50, 0));                                  // remove a missing level
    CHECK(b.set(kBid, 100, 7));                                  // new best bid
    CHECK((all_levels(b.bids) == std::vector<Level>{{100, 7}, {99, 4}, {97, 3}}));
    CHECK(b.set(kAsk, 100, 1));                                  // ask at the best bid's price
    CHECK(b.crossed());
    CHECK(b.set(kAsk, 100, 0));
    CHECK(!b.crossed());

    const std::int8_t side[] = {0, 1, 0, 1};
    const Price price[] = {100, 101, 95, 110};
    const Qty qty[] = {0, 9, 0, 0};                              // 95 and 110 are missing
    CHECK(b.apply(side, price, qty, 4, 11) == 2);
    CHECK(b.last_update_id == 11);
    CHECK(b.bids.best() == std::optional<Level>(Level{99, 4}));
    CHECK(b.asks.best() == std::optional<Level>(Level{101, 9}));

    b.clear();
    CHECK(!b.bids.best() && !b.asks.best() && b.last_update_id == -1 && !b.crossed());
}

// Naive reference side: unsorted vector, linear search, sorted copy on every query.
struct NaiveSide {
    bool is_bid;
    std::vector<Level> levels;

    bool set(Price p, Qty q) {
        auto it = std::find_if(levels.begin(), levels.end(), [p](const Level& l) { return l.price == p; });
        if (q == 0) {
            if (it == levels.end()) return false;
            levels.erase(it);
        } else if (it != levels.end()) {
            it->qty = q;
        } else {
            levels.push_back({p, q});
        }
        return true;
    }

    bool better(Price a, Price b) const { return is_bid ? a > b : a < b; }

    std::vector<Level> sorted_best_first() const {
        std::vector<Level> v = levels;
        std::sort(v.begin(), v.end(), [this](const Level& a, const Level& b) { return better(a.price, b.price); });
        return v;
    }

    std::optional<Level> best() const {  // linear scan, no sort
        std::optional<Level> b;
        for (const Level& l : levels)
            if (!b || better(l.price, b->price)) b = l;
        return b;
    }

    lob::Probe probe(Price p) const {
        lob::Probe r{false, 0};
        for (const Level& l : levels) {
            if (l.price == p) r.found = true;
            if (better(l.price, p)) ++r.better;
        }
        return r;
    }
};

template <class Book>
static void test_random_against_naive(unsigned seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> coin(0, 1), offset(1, 300), pct(0, 99);
    std::uniform_int_distribution<Qty> lots(1, 100000000);
    Book b;
    NaiveSide nb{true, {}}, na{false, {}};
    Price mid = 1000000;
    int mismatches = 0;
    for (int i = 0; i < 200000; ++i) {
        if (i % 1000 == 0) mid += coin(rng) ? 3 : -3;           // slow drift of the price range
        const bool bid = coin(rng) == 1;
        const Price p = bid ? mid - offset(rng) : mid + offset(rng);
        const Qty q = pct(rng) < 35 ? 0 : lots(rng);            // 35 % removals, as an example
        const bool r1 = b.set(bid ? kBid : kAsk, p, q);
        const bool r2 = (bid ? nb : na).set(p, q);
        if (r1 != r2) ++mismatches;
        if (!(b.bids.best() == nb.best()) || !(b.asks.best() == na.best())) ++mismatches;
        if (i % 5000 == 0 &&
            (all_levels(b.bids) != nb.sorted_best_first() || all_levels(b.asks) != na.sorted_best_first()))
            ++mismatches;
    }
    CHECK(mismatches == 0);
    CHECK(all_levels(b.bids) == nb.sorted_best_first());
    CHECK(all_levels(b.asks) == na.sorted_best_first());
}

// Price ladder only: a price path with jumps of up to a window, levels far below the best price and
// many erases of the best level, so that the window moves often in both directions and the std::map
// part is used. After every update: same result and best level as the naive side, and valid()
// (the invariants; O(window), so every 64th update for windows above 1024 slots). Every 1000 updates:
// all levels, and probe() at a few prices.
template <class Book>
static void test_ladder_moves(unsigned seed) {
    using SideT = decltype(Book{}.bids);
    constexpr Price w = SideT::kSlots;
    const Price step = std::max<Price>(w / 64, 1);            // prices on a grid: the book stays small
    const int every = w <= 1024 ? 1 : 64;                      // how often valid() runs
    std::mt19937_64 rng(seed);
    // (not named near / far: <windows.h> defines those as macros)
    std::uniform_int_distribution<int> pct(0, 99), nearby(1, 32), ahead(0, 8), distant(64, 127), jump(-48, 48);
    std::uniform_int_distribution<Qty> lots(1, 100000000);
    Book b;
    NaiveSide nb{true, {}}, na{false, {}};
    Price mid = 1000000 * step;
    int mismatches = 0, invalid = 0;
    for (int i = 0; i < 50000; ++i) {
        if (i % 200 == 0) mid += jump(rng) * step;            // up to 3/4 of the window
        const bool bid = pct(rng) < 50;
        NaiveSide& ref = bid ? nb : na;
        const Price dir = bid ? -1 : 1;                        // away from the mid on this side
        const int r = pct(rng);
        Price p;
        Qty q = lots(rng);
        if (r < 35) {
            p = mid + dir * nearby(rng) * step;                // near the mid
        } else if (r < 42) {
            p = mid - dir * ahead(rng) * step;                 // at or across the mid: a new best price
        } else if (r < 50) {
            p = mid + dir * distant(rng) * step;               // 1 to 2 windows away: below the window
        } else if (r < 70 && !ref.levels.empty()) {
            p = ref.best()->price;                             // erase the best level
            q = 0;
        } else if (r < 90 && !ref.levels.empty()) {
            p = ref.levels[static_cast<std::size_t>(rng() % ref.levels.size())].price;  // erase any level
            q = 0;
        } else {
            p = mid + dir * nearby(rng) * step;                // a removal that may miss
            q = 0;
        }
        if (b.set(bid ? kBid : kAsk, p, q) != ref.set(p, q)) ++mismatches;
        if (!(b.bids.best() == nb.best()) || !(b.asks.best() == na.best())) ++mismatches;
        if (i % every == 0 && (!b.bids.valid() || !b.asks.valid())) ++invalid;
        if (i % 1000 == 999) {
            if (all_levels(b.bids) != nb.sorted_best_first() || all_levels(b.asks) != na.sorted_best_first())
                ++mismatches;
            for (Price x : {mid, mid - 40 * step, mid + 40 * step, mid - 100 * step, mid + 100 * step, p}) {
                const lob::Probe pb = b.bids.probe(x), qb = nb.probe(x), pa = b.asks.probe(x), qa = na.probe(x);
                if (pb.found != qb.found || pb.better != qb.better || pa.found != qa.found || pa.better != qa.better)
                    ++mismatches;
            }
        }
    }
    CHECK(mismatches == 0);
    CHECK(invalid == 0);
    // the test really exercised the window moves and the map, on both sides
    CHECK(b.bids.shifts_up() > 0 && b.bids.shifts_down() > 0 && b.bids.deep_sets() > 0);
    CHECK(b.asks.shifts_up() > 0 && b.asks.shifts_down() > 0 && b.asks.deep_sets() > 0);
}

// Price ladder edge cases with a window of 8 slots: kBestSlot = 5, kLowSlot = 4. Bids: key = price,
// so the window [base, base + 7] is a price range and every expected value can be computed by hand.
static void test_ladder_edge_cases() {
    lob::LadderBookOf<3> b;
    auto& s = b.bids;
    using S = decltype(b.bids);
    static_assert(S::kSlots == 8 && S::kBestSlot == 5 && S::kLowSlot == 4, "the values used below");
    CHECK(!s.set(1000, 0) && !s.best() && s.size() == 0 && s.valid());   // removal on an empty side
    CHECK(s.set(1000, 1));                                  // first level: window [995, 1002], 1000 on slot 5
    CHECK(s.best() == std::optional<Level>(Level{1000, 1}) && s.valid());
    CHECK(s.set(1002, 2) && s.shifts_up() == 0);            // top slot of the window: no move
    CHECK(s.set(1003, 3) && s.shifts_up() == 1);            // above the window: move up to [998, 1005]
    CHECK(s.set(997, 4) && s.deep_sets() == 1);             // below the window: the std::map
    CHECK(!s.set(1010, 0) && s.shifts_up() == 1);           // removal above the window: nothing there, no move
    CHECK((all_levels(s) == std::vector<Level>{{1003, 3}, {1002, 2}, {1000, 1}, {997, 4}}) && s.valid());
    const lob::Probe deep = s.probe(997), above = s.probe(1004), inside = s.probe(1001);
    CHECK(deep.found && deep.better == 3 && !above.found && above.better == 0 && !inside.found && inside.better == 2);
    CHECK(s.set(1003, 0) && s.shifts_down() == 0);          // new best 1002 is on slot 4 = kLowSlot: no move
    CHECK(s.set(1002, 0) && s.shifts_down() == 1);          // best 1000 on slot 2: move down to [995, 1002]
    CHECK(s.best() == std::optional<Level>(Level{1000, 1}) && s.valid());
    CHECK(s.set(1000, 0) && s.shifts_down() == 2);          // array empty: the best (997) comes from the map
    CHECK(s.best() == std::optional<Level>(Level{997, 4}) && s.size() == 1 && s.valid());
    CHECK(s.set(997, 0) && !s.best() && s.size() == 0 && s.valid());

    auto& a = b.asks;                                       // asks: key = -price, a lower ask is better
    CHECK(a.set(2000, 1) && a.set(1998, 2) && a.shifts_up() == 0);   // 2 ticks better: top slot of the window
    CHECK(a.set(1997, 3) && a.shifts_up() == 1);            // 3 ticks better than 2000: above the window
    CHECK(a.best() == std::optional<Level>(Level{1997, 3}) && a.valid());
    CHECK((all_levels(a) == std::vector<Level>{{1997, 3}, {1998, 2}, {2000, 1}}));

    // a snapshot spanning the window and the map; qty-0 levels are skipped
    b.load_snapshot(5, {{1000, 1}, {990, 2}, {998, 0}, {996, 3}}, {{1001, 4}, {1050, 5}});
    CHECK((all_levels(b.bids) == std::vector<Level>{{1000, 1}, {996, 3}, {990, 2}}));
    CHECK((all_levels(b.asks) == std::vector<Level>{{1001, 4}, {1050, 5}}));
    CHECK(b.bids.size() == 3 && b.asks.size() == 2 && b.bids.valid() && b.asks.valid());
    b.clear();
    CHECK(b.bids.size() == 0 && !b.asks.best() && b.bids.valid() && b.asks.valid());
}

int main() {
    test_basic<lob::VecBook>();
    test_basic<lob::MapBook>();
    test_basic<lob::LadderBook>();
    test_basic<lob::LadderBookOf<2>>();   // 4 slots: the window moves at almost every update
    for (unsigned seed : {1u, 2u, 3u}) {
        test_random_against_naive<lob::VecBook>(seed);
        test_random_against_naive<lob::MapBook>(seed);
        test_random_against_naive<lob::LadderBook>(seed);
        test_random_against_naive<lob::LadderBookOf<3>>(seed);
        test_ladder_moves<lob::LadderBookOf<3>>(seed);
        test_ladder_moves<lob::LadderBookOf<10>>(seed);
        test_ladder_moves<lob::LadderBook>(seed);
    }
    test_ladder_edge_cases();
    std::printf("%d checks, %d failed\n", g_checks, g_failed);
    return g_failed == 0 ? 0 : 1;
}