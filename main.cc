#include <butil/logging.h>
#include <gflags/gflags.h>

#include <thread>
#include <vector>

#include "Paxos.h"

DEFINE_int32(server_num, 5, "the server num which run Paxos");
DEFINE_bool(simulate_network_error, true,
            "where to simulate network error, such as network delay");

int main(int argc, char* argv[]) {
  GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
  std::vector<Paxos::PaxosServer*> servers;
  if (StartServers(FLAGS_server_num, &servers) != 0) {
    LOG(ERROR) << "start server failed";
    return 0;
  }

  std::string value;
  Paxos::Status status;

  // case1: 1个server发起Paxos
  LOG(INFO) << "start Put foo1 bar1";
  Paxos::PaxosInstance paxos_instance1(&servers);
  status = paxos_instance1.Put("foo1", "bar1");
  LOG(INFO) << "end Put foo1 bar1, error_code: " << static_cast<int>(status);

  LOG(INFO) << "start Get foo1";
  Paxos::PaxosInstance paxos_instance2(&servers);
  status = paxos_instance2.Get("foo1", &value);
  LOG(INFO) << "end Get foo1, value: " << value
            << ", error_code: " << static_cast<int>(status);

  // case2: 多个server同时发起Paxos
  int server_nums_at_sametime = 5;
  std::vector<std::thread> threads;
  for (int server_num = 2; server_num <= server_nums_at_sametime;
       server_num++) {
    threads.clear();
    LOG(INFO) << server_num << "'s request propose at same time begin";
    for (int i = 0; i < server_num; i++) {
      threads.emplace_back(std::thread([&] {
        std::string value;
        Paxos::Status status;
        std::thread::id this_id = std::this_thread::get_id();
        LOG(INFO) << this_id << " start Put foo2 bar2";
        Paxos::PaxosInstance paxos_instance3(&servers);
        status = paxos_instance1.Put("foo2", "bar2");
        LOG(INFO) << this_id << " end Put foo2 bar2, error_code: "
                  << static_cast<int>(status);

        LOG(INFO) << this_id << " start Get foo2";
        Paxos::PaxosInstance paxos_instance4(&servers);
        status = paxos_instance2.Get("foo2", &value);
        LOG(INFO) << this_id << " end Get foo2, value: " << value
                  << ", error_code: " << static_cast<int>(status);
      }));
    }

    for (auto&& thread : threads) {
      thread.join();
    }
    LOG(INFO) << server_num << "'s request propose at same time end";
  }

  StopServers(&servers);
  return 0;
}