#include <iostream>
#include <fstream>
#include <sstream>
#include <functional>

#include <psl/net.h>
#include <psl/log.h>
#include <psl/terminal.h>
#include <psl/type_traits.h>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

enum LockMode {
    NL,
    X,
    S,
    IX,
    IS,
    SIX,
    SIZE
};

/* upgrade[old][new] = upgraded mode */
static LockMode upgrade[LockMode::SIZE][LockMode::SIZE] = {
    [LockMode::NL] = {
        [LockMode::NL] = LockMode::NL,
        [LockMode::X] = LockMode::X,
        [LockMode::S] = LockMode::S,
        [LockMode::IX] = LockMode::IX,
        [LockMode::IS] = LockMode::IS,
        [LockMode::SIX] = LockMode::SIX
    },
    [LockMode::X] = {
        [LockMode::NL] = LockMode::X,
        [LockMode::X] = LockMode::X,
        [LockMode::S] = LockMode::X,
        [LockMode::IX] = LockMode::X,
        [LockMode::IS] = LockMode::X,
        [LockMode::SIX] = LockMode::X
    },
    [LockMode::S] = {
        [LockMode::NL] = LockMode::S,
        [LockMode::X] = LockMode::X,
        [LockMode::S] = LockMode::S,
        [LockMode::IX] = LockMode::SIX,
        [LockMode::IS] = LockMode::S,
        [LockMode::SIX] = LockMode::SIX
    },
    [LockMode::IX] = {
        [LockMode::NL] = LockMode::IX,
        [LockMode::X] = LockMode::X,
        [LockMode::S] = LockMode::SIX,
        [LockMode::IX] = LockMode::IX,
        [LockMode::IS] = LockMode::IX,
        [LockMode::SIX] = LockMode::SIX
    },
    [LockMode::IS] = {
        [LockMode::NL] = LockMode::IS,
        [LockMode::X] = LockMode::X,
        [LockMode::S] = LockMode::S,
        [LockMode::IX] = LockMode::IX,
        [LockMode::IS] = LockMode::IS,
        [LockMode::SIX] = LockMode::SIX
    },
    [LockMode::SIX] = {
        [LockMode::NL] = LockMode::SIX,
        [LockMode::X] = LockMode::X,
        [LockMode::S] = LockMode::SIX,
        [LockMode::IX] = LockMode::SIX,
        [LockMode::IS] = LockMode::SIX,
        [LockMode::SIX] = LockMode::SIX
    }
};

inline std::ostream& operator<<(std::ostream& out, LockMode mode) {
    static const char* lockmode_to_str[] = {
        [LockMode::NL] = "NL",
        [LockMode::X] = "X",
        [LockMode::S] = "S",
        [LockMode::IX] = "IX",
        [LockMode::IS] = "IS",
        [LockMode::SIX] = "SIX"
    };
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

inline std::istream& operator>>(std::istream& in, LockMode& mode) {
    std::string str;
    in >> str;
    if (str == "X") {
        mode = LockMode::X;
    } else if(str == "S") {
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

struct Lock {
    LockKey key;
    std::string dbg_str;
    LockMode mode;
    std::list<Lock>::iterator prev;
};

inline bool operator==(const Lock& a, const LockKey& key) {
    return a.key == key;
}

struct Transaction {
    uint64_t id;
    std::list<Lock> lock;
    std::vector<decltype(lock)::iterator> unlock;
};

static std::vector<Transaction> trxs;

int parse_lock_log(std::istream& in, size_t record_lock_limit) {
    std::multimap<size_t, decltype(Transaction::lock)::iterator> table_map;
    bool lock = false;
    decltype(trxs)::iterator trx;
    while (in.good()) {
        std::string line;
        std::getline(in, line);
        if (!line.size()) {
            continue;
        }
        std::deque<std::string> strs;
        boost::split(strs, line, boost::is_any_of(" "));
        LOG_ERR_EXIT(strs.size() < 9, EINVAL, std::system_category());

        if(strs[0] == "UNLOCK") {
            lock = false;
            strs.pop_front();
        } else {
            if (!lock) {
                /* 2PL -> started a new transaction! */
                table_map.clear();
                trx = trxs.insert(trxs.end(), Transaction{0});
            }
            lock = true;
        }

        bool record;
        if (strs[0] == "TABLE") {
            LOG_ERR_EXIT(strs.size() != 9, EINVAL, std::system_category());
            record = false;
        } else if(strs[0] == "RECORD") {
            LOG_ERR_EXIT(strs.size() != 18, EINVAL, std::system_category());
            record = true;
        } else {
            LOG_ERR_EXIT(true, EINVAL, std::system_category());
        }

        std::stringstream ss(strs[record ? 15 : 6]);
        uint64_t trx_id;
        ss >> trx_id;
        LOG_ERR_EXIT(ss.fail(), EINVAL, std::system_category());
        LOG_ERR_EXIT(trx->id != 0 && trx_id != trx->id, EINVAL, std::system_category());
        trx->id = trx_id;

        std::hash<std::string> hashfn;
        LockKey key;
        std::string dbg_str = record ? strs[3] + ' ' + strs[5] + ' ' + strs[7] : strs[3];
        key = hashfn(dbg_str);

        LockMode mode;
        ss.clear();
        ss.str(strs[record ? 17 : 8]);
        ss >> mode;
        LOG_ERR_EXIT(ss.fail(), EINVAL, std::system_category());

        if (lock) {
            auto lockit = std::find(trx->lock.rbegin(), trx->lock.rend(), key);
            bool insert = true;
            if(lockit != trx->lock.rend()) {
                auto new_mode = lockit->mode + mode;
                if (new_mode != lockit->mode) {
                    /* need new lock */
                    // std::cout << "(" << lockit->mode << "+" << mode
                    //     << ") -> " << new_mode << '\n';
                    mode = new_mode;
                } else {
                    insert = false;
                }
            }
            if (insert) {
                if (record) {
                    size_t table_key = hashfn(strs[12]);
                    auto table_lock = std::find(trx->lock.begin(), trx->lock.end(), table_key);
                    LOG_ERR_EXIT(table_lock == trx->lock.end(), EINVAL, std::system_category());

                    /* if we locked the table S or X we do not need to take the
                     * record lock anymore (record_limit optimization)
                     * checking if things are compatible is not our job! */
                    if (    table_lock->mode == LockMode::IS ||
                            table_lock->mode == LockMode::IX ||
                            (table_lock->mode == LockMode::SIX && mode == LockMode::X)) {
                        auto table_range = table_map.equal_range(table_key);
                        if (std::distance(table_range.first, table_range.second) > record_lock_limit) {
                            /* 1. remove all records locks of this table
                             * (including the on we just inserted)
                             * 2. remove all table locks expect first
                             * 2. upgrade first table lock (S|X)! */
                            for (auto to_delete = table_range.first; to_delete != table_range.second;) {
                                trx->lock.erase(to_delete->second);
                                to_delete = table_map.erase(to_delete);
                            }

                            LockMode new_mode = LockMode::NL;
                            for (auto prev = table_lock->prev;
                                    prev != trx->lock.end(); prev = table_lock->prev) {
                                new_mode += table_lock->mode;
                                trx->lock.erase(table_lock);
                                table_lock = prev;
                            }
                            table_lock->mode += new_mode;
                            /* we need at least S */
                            table_lock->mode += LockMode::S;
                         } else {
                            auto new_lock = trx->lock.insert(trx->lock.end(),
                                    {key, dbg_str + " " + strs[12] ,mode});
                            table_map.insert({table_key, new_lock});
                         }
                    }
                } else {
                    trx->lock.push_back({key, dbg_str, mode, trx->lock.end()});
                }
            }
        } else if (!record) { /* unlock table (unlocks also record locks) */
            /* ASSUMPTION: SS2PL, i.e. all locks are released after commit:
             * 1) No unlock order (other than record before table)
             * 2) locks are not downgraded (X -> S) etc. always unlock in one go */

            auto it = std::find(trx->lock.begin(), trx->lock.end(), key);
            LOG_ERR_EXIT(it == trx->lock.end(), EINVAL, std::system_category());
            if (std::find(trx->unlock.begin(), trx->unlock.end(), it)
                    == trx->unlock.end()) {
                /* release all locks belonging to this table */
                auto table_range = table_map.equal_range(key);
                if (table_range.first != table_range.second) {
                    table_range.second--;
                    for (auto l = table_range.second; l != table_range.first; l--) {
                        trx->unlock.emplace_back(l->second);
                    }
                    trx->unlock.emplace_back(table_range.first->second);
                }
                trx->unlock.emplace_back(it);
            }
        }
    }
    return 0;
}

int main(int argc, char* argv[]) {
    namespace bop = boost::program_options;

    bop::options_description desc("Options");
    desc.add_options()
        ("help", "produce this message")
        ("ip", bop::value<psl::net::in_addr>()->required(), "server ip")
        ("p", bop::value<psl::net::in_port_t>()->default_value(1234), "server port")
        ("l", bop::value<std::string>()->required(), "lock log-file")
        ("q", bop::value<std::string>(), "query log-file")
        ("limit", bop::value<size_t>()->default_value(std::numeric_limits<size_t>::max()),
         "transaction record lock limit -> upgrade to table lock if reached");

    bop::variables_map vm;
    bop::store(
        bop::command_line_parser(argc, argv).options(desc).run(),
        vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }
    bop::notify(vm);

    // psl::net::in_addr ip = vm["ip"].as<psl::net::in_addr>();
    // psl::net::in_port_t port = vm["p"].as<psl::net::in_port_t>();

    std::string lock_log_path = vm["l"].as<std::string>();
    std::fstream lock_log(lock_log_path);

    LOG_ERR_EXIT(!lock_log.good(), EINVAL, std::system_category());

    int ret;
    LOG_ERR_EXIT((ret = parse_lock_log(lock_log, vm["limit"].as<size_t>())), ret,
            std::system_category());

    uint64_t n_locks_avg = 0;
    uint32_t nn = 0;
    for (auto& trx : trxs) {
        std::cout << "---------------------------------------------------\n";
        std::cout << "Transaction " << trx.id << '\n';
        std::cout << "---------------------------------------------------\n";
        std::cout << "n_locks: " << trx.lock.size() << '\n';
        n_locks_avg += trx.lock.size();
        std::cout << "n_unlocks: " << trx.unlock.size() << '\n';
        for (auto& l : trx.lock) {
            std::cout << "LOCK " << l.dbg_str << " ["
                << l.mode << "]\n";
        }
        for (auto ul : trx.unlock) {
            std::cout << "UNLOCK " << ul->dbg_str << '\n';
        }
        nn++;
    }

    std::cout << "lock_avg: " << n_locks_avg / static_cast<double>(nn) << '\n';

    return 0;
}
