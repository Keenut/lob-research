// test_book.cpp: C++ unit tests for book.hpp, without an external test framework.
//
// Every test runs on both containers (VecBook and MapBook). A randomized test also compares them with
// a third, deliberately naive implementation (unsorted vector, linear search, sort on every query),
// so a bug shared by the two real containers is unlikely to go unnoticed.
//
// Build and run (PowerShell, from the repo root):
//     cmake --build build --config Release
//     .\build\Release\lob_tests.exe
#include <algorithm>
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

    std::vector<Level> sorted_best_first() const {
        std::vector<Level> v = levels;
        std::sort(v.begin(), v.end(), [this](const Level& a, const Level& b) {
            return is_bid ? a.price > b.price : a.price < b.price;
        });
        return v;
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
        const auto ref_b = nb.sorted_best_first();
        const auto ref_a = na.sorted_best_first();
        const std::optional<Level> exp_b = ref_b.empty() ? std::nullopt : std::optional<Level>(ref_b.front());
        const std::optional<Level> exp_a = ref_a.empty() ? std::nullopt : std::optional<Level>(ref_a.front());
        if (!(b.bids.best() == exp_b) || !(b.asks.best() == exp_a)) ++mismatches;
        if (i % 5000 == 0 && (all_levels(b.bids) != ref_b || all_levels(b.asks) != ref_a)) ++mismatches;
    }
    CHECK(mismatches == 0);
    CHECK(all_levels(b.bids) == nb.sorted_best_first());
    CHECK(all_levels(b.asks) == na.sorted_best_first());
}

int main() {
    test_basic<lob::VecBook>();
    test_basic<lob::MapBook>();
    for (unsigned seed : {1u, 2u, 3u}) {
        test_random_against_naive<lob::VecBook>(seed);
        test_random_against_naive<lob::MapBook>(seed);
    }
    std::printf("%d checks, %d failed\n", g_checks, g_failed);
    return g_failed == 0 ? 0 : 1;
}