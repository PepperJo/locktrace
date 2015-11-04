#include <iostream>
#include <fstream>
#include <sstream>
#include <functional>

#include <psl/net.h>
#include <psl/log.h>
#include <psl/terminal.h>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

enum class LockMode {
    X, S, IX, IS
};

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
    LockMode mode;
};

struct Transaction {
    uint64_t id;
    std::vector<Lock> lock;
    std::vector<LockKey> unlock;
};

std::vector<Transaction> trxs;

int parse_lock_log(std::istream& in) {
    std::multimap<std::pair<size_t, size_t>, Lock> page_map;
    std::set<LockKey> lock_set;
    bool unlock = true;
    decltype(trxs)::iterator trx;
    while (in.good()) {
        std::string line;
        std::getline(in, line);
        std::deque<std::string> strs;
        boost::split(strs, line, boost::is_any_of(" "));
        LOG_ERR_EXIT(strs.size() < 9, EINVAL, std::system_category());

        if(strs[0] == "UNLOCK") {
            unlock = true;
            strs.pop_front();
        } else {
            if (unlock) {
                /* 2PL -> started a new transaction! */
                trx = trxs.insert(trxs.end(), Transaction{0});
            }
            unlock = false;
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
        key = hashfn(record ? strs[3]+ strs[5] + strs[7] : strs[4]);

        if (unlock) {
            if (record) {
                /* release all the record from this page! */
                // page_map.find()
            } else {
                trx->unlock.push_back(key);
            }
        } else {
            Lock lock;
            lock.key = key;
            ss.clear();
            ss.str(strs[record ? 17 : 8]);
            ss >> lock.mode;
            LOG_ERR_EXIT(ss.fail(), EINVAL, std::system_category());
            bool inserted = lock_set.insert(key).second;
            if (!inserted) {
                /* do we need to upgrade the lock? */
            }
            trx->lock.push_back(lock);
            if (record) {
                size_t space, page_no;
                ss.clear();
                ss.str(strs[3]);
                ss >> space;
                LOG_ERR_EXIT(ss.fail(), EINVAL, std::system_category());
                ss.clear();
                ss.str(strs[5]);
                ss >> page_no;
                LOG_ERR_EXIT(ss.fail(), EINVAL, std::system_category());
                page_map.insert({{space, page_no}, lock});
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
        ("q", bop::value<std::string>(), "query log-file");

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
    LOG_ERR_EXIT((ret = parse_lock_log(lock_log)), ret, std::system_category());

    return 0;
}
