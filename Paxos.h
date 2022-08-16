#include <brpc/server.h>
#include <butil/rand_util.h>

#include <mutex>
#include <string>
#include <unordered_map>

#include "Paxos.pb.h"

namespace Paxos {

uint64_t SetAndGetGlobalSeq(uint64_t seq);

enum class Status {
  kOK = 0,
  kInit = 1,
  kReject = 2,
  kNotFound = 3,
};

class PaxosImpl : public Paxos {
 public:
  explicit PaxosImpl(uint64_t node_id) : node_id_(node_id){};
  virtual ~PaxosImpl(){};

  virtual void Prepare(google::protobuf::RpcController* base_cntl,
                       const PrepareRequest* request, PrepareResponse* response,
                       google::protobuf::Closure* done);
  virtual void Accept(google::protobuf::RpcController* base_cntl,
                      const AcceptRequest* request, AcceptResponse* response,
                      google::protobuf::Closure* done);

 private:
  struct Version {
    Ballot last_bal;
    Value val;
    Ballot v_bal;

    Version() {
      last_bal.set_seq(0);
      val.set_val("");
      v_bal.set_seq(0);
    }
  };

  uint64_t node_id_;
  std::mutex mtx_for_storage_;
  std::unordered_map<std::string, Version> storage_;
};

class PaxosServer {
 public:
  explicit PaxosServer(uint64_t node_id) : node_id_(node_id) {}
  ~PaxosServer() = default;

  int Start();

  void Stop();

  std::string GetServerAddr() const;

  Status RunPaxos(uint64_t seq, const std::string& key,
                  const std::string& in_value, std::string* out_value,
                  std::vector<PaxosServer*>* servers);

  void PreparePhase(uint64_t seq_num, const std::string& key,
                    std::vector<PaxosServer*>* servers,
                    std::vector<PrepareResponse>* result);

  void AcceptPhase(uint64_t seq_num, const std::string& key,
                   const std::string& in_value,
                   std::vector<PaxosServer*>* servers,
                   std::vector<AcceptResponse>* result);

 private:
  Status OncePaxos(uint64_t in_seq, uint64_t* out_seq, const std::string& key,
                   const std::string& in_value, std::string* out_value,
                   std::vector<PaxosServer*>* servers);

  uint64_t node_id_;
  brpc::Server server_;
  std::mutex mtx_;
};

int StartServers(int num, std::vector<PaxosServer*>* servers);
void StopServers(std::vector<PaxosServer*>* servers);

class PaxosInstance {
 public:
  explicit PaxosInstance(std::vector<PaxosServer*>* servers)
      : servers_(servers) {}
  ~PaxosInstance() = default;

  Status Put(const std::string& key, const std::string& value) {
    return RunPaxos(butil::RandInt(0, servers_->size() - 1),
                    SetAndGetGlobalSeq(0), key, value, nullptr);
  }

  Status Get(const std::string& key, std::string* value) {
    return RunPaxos(butil::RandInt(0, servers_->size() - 1),
                    SetAndGetGlobalSeq(0), key, {}, value);
  }

 private:
  Status RunPaxos(int node_id, uint64_t seq, const std::string& key,
                  const std::string& in_value, std::string* out_value) {
    return (*servers_)[node_id]->RunPaxos(seq, key, in_value, out_value,
                                          servers_);
  }

  std::vector<PaxosServer*>* servers_;
};

}  // namespace Paxos