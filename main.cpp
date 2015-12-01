#include <iostream>
#include <fstream>
#include <sstream>
#include <functional>
#include <iterator>
#include <regex>
#include <string>

#include <psl/net.h>
#include <psl/log.h>
#include <psl/terminal.h>
#include <psl/type_traits.h>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>

enum LockMode { NL, X, S, IX, IS, SIX, SIZE };

// clang-format off
/* upgrade[old][new] = upgraded mode */
static const LockMode upgrade[LockMode::SIZE][LockMode::SIZE] =
    {[LockMode::NL] = {[LockMode::NL] = LockMode::NL,
                       [LockMode::X] = LockMode::X,
                       [LockMode::S] = LockMode::S,
                       [LockMode::IX] = LockMode::IX,
                       [LockMode::IS] = LockMode::IS,
                       [LockMode::SIX] = LockMode::SIX},
     [LockMode::X] = {[LockMode::NL] = LockMode::X,
                      [LockMode::X] = LockMode::X,
                      [LockMode::S] = LockMode::X,
                      [LockMode::IX] = LockMode::X,
                      [LockMode::IS] = LockMode::X,
                      [LockMode::SIX] = LockMode::X},
     [LockMode::S] = {[LockMode::NL] = LockMode::S,
                      [LockMode::X] = LockMode::X,
                      [LockMode::S] = LockMode::S,
                      [LockMode::IX] = LockMode::SIX,
                      [LockMode::IS] = LockMode::S,
                      [LockMode::SIX] = LockMode::SIX},
     [LockMode::IX] = {[LockMode::NL] = LockMode::IX,
                       [LockMode::X] = LockMode::X,
                       [LockMode::S] = LockMode::SIX,
                       [LockMode::IX] = LockMode::IX,
                       [LockMode::IS] = LockMode::IX,
                       [LockMode::SIX] = LockMode::SIX},
     [LockMode::IS] = {[LockMode::NL] = LockMode::IS,
                       [LockMode::X] = LockMode::X,
                       [LockMode::S] = LockMode::S,
                       [LockMode::IX] = LockMode::IX,
                       [LockMode::IS] = LockMode::IS,
                       [LockMode::SIX] = LockMode::SIX},
     [LockMode::SIX] = {[LockMode::NL] = LockMode::SIX,
                        [LockMode::X] = LockMode::X,
                        [LockMode::S] = LockMode::SIX,
                        [LockMode::IX] = LockMode::SIX,
                        [LockMode::IS] = LockMode::SIX,
                        [LockMode::SIX] = LockMode::SIX}};

static const bool compatible[LockMode::SIZE][LockMode::SIZE] =
    {[LockMode::NL] = {[LockMode::NL] = true,
                       [LockMode::X] = true,
                       [LockMode::S] = true,
                       [LockMode::IX] = true,
                       [LockMode::IS] = true,
                       [LockMode::SIX] = true},
     [LockMode::X] = {[LockMode::NL] = true,
                      [LockMode::X] = false,
                      [LockMode::S] = false,
                      [LockMode::IX] = false,
                      [LockMode::IS] = false,
                      [LockMode::SIX] = false},
     [LockMode::S] = {[LockMode::NL] = true,
                      [LockMode::X] = false,
                      [LockMode::S] = true,
                      [LockMode::IX] = false,
                      [LockMode::IS] = true,
                      [LockMode::SIX] = false},
     [LockMode::IX] = {[LockMode::NL] = true,
                       [LockMode::X] = false,
                       [LockMode::S] = false,
                       [LockMode::IX] = true,
                       [LockMode::IS] = true,
                       [LockMode::SIX] = false},
     [LockMode::IS] = {[LockMode::NL] = true,
                       [LockMode::X] = false,
                       [LockMode::S] = true,
                       [LockMode::IX] = true,
                       [LockMode::IS] = true,
                       [LockMode::SIX] = true},
     [LockMode::SIX] = {[LockMode::NL] = true,
                        [LockMode::X] = false,
                        [LockMode::S] = false,
                        [LockMode::IX] = false,
                        [LockMode::IS] = true,
                        [LockMode::SIX] = false}};
// clang-format on

inline std::ostream& operator<<(std::ostream& out, LockMode mode) {
    static const char* lockmode_to_str[] = {[LockMode::NL] = "NL",
                                            [LockMode::X] = "X",
                                            [LockMode::S] = "S",
                                            [LockMode::IX] = "IX",
                                            [LockMode::IS] = "IS",
                                            [LockMode::SIX] = "SIX"};
    if (mode < 0 || mode >= LockMode::SIZE) {
        out.setstate(std::ios_base::failbit);
    } else {
        out << lockmode_to_str[mode];
    }
    return out;
}

inline LockMode operator+(const LockMode a, const LockMode b) {
    return upgrade[a][b];
}

inline LockMode operator+=(LockMode& a, const LockMode b) {
    a = a + b;
    return a;
}

inline bool operator&&(const LockMode a, const LockMode b) {
    return compatible[a][b];
}

inline std::istream& operator>>(std::istream& in, LockMode& mode) {
    std::string str;
    in >> str;
    if (str == "X") {
        mode = LockMode::X;
    } else if (str == "S") {
        mode = LockMode::S;
    } else if (str == "IX") {
        mode = LockMode::IX;
    } else if (str == "IS") {
        mode = LockMode::IS;
    } else {
        in.setstate(std::ios_base::failbit);
    }
    return in;
}

using LockKey = uint64_t;
struct Lock;
using LockContainer = std::list<Lock>;

struct Lock {
    LockKey key;
    std::string dbg_str;
    LockMode mode;
    LockContainer::iterator prev;
    std::string extra;
};

inline bool operator==(const Lock& a, const LockKey& key) {
    return a.key == key;
}

struct Transaction {
    uint64_t id;
    LockContainer lock;
    std::multimap<size_t, decltype(Transaction::lock)::iterator> table_map;
};

template<class T>
static std::string parse_query_and_match(const T& strs,
        std::string regex_str) {
    bool insert = false;
    typename T::const_iterator iter = strs.end();
    if (strs[1] == "SELECT" || strs[1] == "UPDATE" || strs[1] == "DELETE") {
        iter = std::find(strs.begin(), strs.end(), "WHERE");
    } else if (strs[1] == "INSERT") {
        iter = strs.begin() + 2;
        insert = true;
    }

    boost::regex r(regex_str);
    for (; iter != strs.end(); iter++)  {
        if (boost::regex_search(*iter, r)) {
            break;
        }
    }
    if (iter != strs.end()) {
        if (insert) {
            size_t i = std::distance(strs.begin(), iter);
            i -= 3;
            iter = std::find(iter, strs.end(), "VALUES");
            if (std::distance(iter, strs.end()) > i) {
                std::string value = *(iter + i);
                value.pop_back();
                return value;
            }
        } else {
            return *(iter + 2);
        }
    }
    return "";
}

template<class Iter>
static bool record_to_table_lock(Transaction& trx, Iter table_lock,
        size_t record_lock_limit) {
    auto table_range =
        trx.table_map.equal_range(table_lock->key);
    if (std::distance(table_range.first,
                      table_range.second) >
        record_lock_limit) {
        /* 1. remove all records locks of this table
         * (including the on we just inserted)
         * 2. remove all table locks expect first
         * 2. upgrade first table lock (S|X)! */
        for (auto to_delete = table_range.first;
             to_delete != table_range.second;) {
            trx.lock.erase(to_delete->second);
            to_delete = trx.table_map.erase(to_delete);
        }

        LockMode new_mode = LockMode::NL;
        auto ftable_lock = std::next(table_lock).base();
        for (auto prev = ftable_lock->prev;
             prev != trx.lock.end();
             prev = ftable_lock->prev) {
            new_mode += ftable_lock->mode;
            trx.lock.erase(ftable_lock);
            ftable_lock = prev;
        }
        ftable_lock->mode += new_mode;
        /* we need at least S */
        ftable_lock->mode += LockMode::S;
        return true;
    }

    return false;
}

template<class T>
static int parse_lock(const T& strs, Transaction& trx,
        size_t record_lock_limit, std::string extra, bool direct_upgrade) {
    bool record;
    if (strs[0] == "TABLE") {
        LOG_ERR_EXIT(strs.size() < 9, EINVAL, std::system_category());
        record = false;
    } else if (strs[0] == "RECORD") {
        LOG_ERR_EXIT(strs.size() < 18, EINVAL, std::system_category());
        record = true;
    } else {
        LOG_ERR_EXIT(true, EINVAL, std::system_category());
    }

    std::stringstream ss(strs[record ? 15 : 6]);
    uint64_t trx_id;
    ss >> trx_id;
    LOG_ERR_EXIT(ss.fail(), EINVAL, std::system_category());
    LOG_ERR_EXIT(trx.id != 0 && trx_id != trx.id, EINVAL,
                 std::system_category());
    trx.id = trx_id;

    std::hash<std::string> hashfn;
    LockKey key;
    std::string dbg_str =
        (record ? strs[3] + ' ' + strs[5] + ' ' + strs[7] : strs[3]);
    dbg_str += ' ' + extra;
    key = hashfn(dbg_str);

    LockMode mode;
    ss.clear();
    ss.str(strs[record ? 17 : 8]);
    ss >> mode;
    LOG_ERR_EXIT(ss.fail(), EINVAL, std::system_category());

    auto lockit = std::find(trx.lock.rbegin(), trx.lock.rend(), key);
    bool insert = true;
    if (lockit != trx.lock.rend()) {
        /* only take new lock if we have a stricter mode */
        auto new_mode = lockit->mode + mode;
        if (new_mode != lockit->mode) {
            mode = new_mode;
            /* if we cannot upgrade locks and locks are not compatible
             * upgrade lock directly*/
            if (direct_upgrade && !(mode && lockit->mode)) {
                lockit->mode = mode;
                insert = false;
            }
        } else {
            insert = false;
        }
    }
    if (insert) {
        if (record) {
            std::string table_id = strs[12] + ' ' + extra;
            auto table_lock = std::find(
                trx.lock.rbegin(), trx.lock.rend(), hashfn(table_id));
            LOG_ERR_EXIT(table_lock == trx.lock.rend(), EINVAL,
                         std::system_category());

            /* if we locked the table S or X we do not need to take the
             * record lock anymore (record_limit optimization)
             * checking if things are compatible is not our job! */
            if (table_lock->mode == LockMode::IS ||
                table_lock->mode == LockMode::IX ||
                (table_lock->mode == LockMode::SIX &&
                 mode == LockMode::X)) {
                if (!record_to_table_lock(trx, table_lock, record_lock_limit)) {
                    auto table_key = table_lock->key;
                    auto new_lock = trx.lock.insert(
                        trx.lock.end(),
                        {key, dbg_str + " " + strs[12], mode,
                         std::next(lockit).base(), extra});
                    trx.table_map.insert({table_key, new_lock});
                }
            }
        } else {
            trx.lock.push_back(
                {key, dbg_str, mode, std::next(lockit).base(), extra});
        }
    }
    return 0;
}

template<class T>
static int parse_lock_log(std::istream& in, T& trxs, size_t record_lock_limit,
        const std::string regex_str, bool direct_upgrade) {
    bool lock = false;
    typename T::iterator trx;
    std::string extra;
    while (in.good()) {
        std::string line;
        std::getline(in, line);
        if (!line.size()) {
            continue;
        }
        std::vector<std::string> strs;
        boost::split(strs, line, boost::is_any_of(" "));
        LOG_ERR_EXIT(strs.size() < 2, EINVAL, std::system_category());

        if (strs[0] == "UNLOCK") {
            lock = false;
        } else if (strs[1] == "LOCK") {
            if (!lock) {
                /* 2PL -> started a new transaction! */
                trx = trxs.insert(trxs.end(), Transaction{0, {}, {}});
            }
            lock = true;
            parse_lock(strs, *trx, record_lock_limit, extra, direct_upgrade);
        } else if (strs[0] == "QUERY") {
            if (regex_str != "") {
                extra = parse_query_and_match(strs, regex_str);
            }
        } else {
            std::cout << line << '\n';
            LOG_ERR_EXIT(true, EINVAL, std::system_category());
        }

    }
    return 0;
}

int main(int argc, char* argv[]) {
    namespace bop = boost::program_options;

    bop::options_description desc("Options");
    // clang-format off
    desc.add_options()
        ("help", "produce this message")
        ("l", bop::value<std::string>()->required(), "lock log-file")
        ("limit",
         bop::value<size_t>()->default_value(std::numeric_limits<size_t>::max()),
         "transaction record lock limit -> upgrade to table lock if reached")
        ("match", bop::value<std::string>()->default_value(""),
         "regex to match for extra")
        ("direct_upgrade", "upgrade locks directly");
    // clang-format on

    bop::variables_map vm;
    bop::store(bop::command_line_parser(argc, argv).options(desc).run(), vm);

    if (vm.count("help")) {
    std::cout << desc << "\n";
    return 1;
    }
    bop::notify(vm);

    std::string lock_log_path = vm["l"].as<std::string>();
    std::fstream lock_log(lock_log_path);
    LOG_ERR_EXIT(!lock_log.good(), EINVAL, std::system_category());

    std::vector<Transaction> trxs;
    int ret;
    LOG_ERR_EXIT((ret = parse_lock_log(lock_log, trxs, vm["limit"].as<size_t>(),
                                     vm["match"].as<std::string>(),
                                     vm.count("direct_upgrade"))),
               ret, std::system_category());

#ifdef DEBUG
    uint64_t n_locks_avg = 0;
    uint32_t nn = 0;
    for (auto& trx : trxs) {
        std::cout << "---------------------------------------------------\n";
        std::cout << "Transaction " << trx.id << '\n';
        std::cout << "---------------------------------------------------\n";
        std::cout << "n_locks: " << trx.lock.size() << '\n';
        n_locks_avg += trx.lock.size();
        for (auto &l : trx.lock) {
          std::cout << "LOCK " << l.dbg_str << " [" << l.mode << "]\n";
        }
        nn++;
    }

    std::cout << "transactions: " << trxs.size() << '\n';
    std::cout << "lock_avg: " << n_locks_avg / static_cast<double>(nn) << '\n';
#endif

    for (auto& trx : trxs) {
        std::cout << "> " << trx.id << '\n';
        for (auto& l : trx.lock) {
            std::cout << l.key << ' ' << l.mode << ' ' << l.extra << '\n';
        }
    }

    return 0;
}
