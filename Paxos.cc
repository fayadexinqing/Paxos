#include "Paxos.h"

#include <brpc/channel.h>
#include <butil/logging.h>

#include <algorithm>
#include <chrono>
#include <thread>

DECLARE_bool(simulate_network_error);

namespace Paxos {

constexpr int global_base_port = 6000;

uint64_t SetAndGetGlobalSeq(uint64_t seq) {
  static std::mutex mtx;
  static uint64_t global_seq = 1;
  std::unique_lock<std::mutex> lk;
  if (global_seq < seq) {
    global_seq = seq;
    return global_seq;
  }
  return global_seq++;
}

int StartServers(int num, std::vector<PaxosServer*>* servers) {
  brpc::ServerOptions options;
  for (int id = 0; id < num; id++) {
    PaxosServer* server = new PaxosServer(id);
    if (server->Start() != 0) {
      LOG(ERROR) << "server[" << id << "] start failed";
      return -1;
    }
    servers->push_back(server);
  }
  return 0;
}

void StopServers(std::vector<PaxosServer*>* servers) {
  for (auto server : *servers) {
    server->Stop();
    delete server;
  }
  servers->clear();
}

void PaxosImpl::Prepare(google::protobuf::RpcController* base_cntl,
                        const PrepareRequest* request,
                        PrepareResponse* response,
                        google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(base_cntl);
  if (FLAGS_simulate_network_error) {
    uint64_t sleep_time = butil::RandInt(0, 1200);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
  }

  std::string key = request->id().key();
  std::unique_lock<std::mutex> lk(mtx_for_storage_);
  auto it = storage_.find(key);
  if (it == storage_.end()) {
    Version ver;
    ver.last_bal = request->bal();
    storage_[key] = ver;
  } else {
    if (request->bal().seq() >= it->second.last_bal.seq()) {
      it->second.last_bal = request->bal();
    }
  }

  const Version& ver = storage_[key];
  Ballot* last_bal = new Ballot(ver.last_bal);
  response->set_allocated_last_bal(last_bal);
  Value* val = new Value(ver.val);
  response->set_allocated_val(val);
  Ballot* v_bal = new Ballot(ver.v_bal);
  response->set_allocated_v_bal(v_bal);
  LOG(INFO) << "server[" << node_id_ << "] receive Prepare request from "
            << cntl->remote_side() << " to " << cntl->local_side()
            << ", request : " << request->ShortDebugString()
            << ", response: " << response->ShortDebugString();
}

void PaxosImpl::Accept(google::protobuf::RpcController* base_cntl,
                       const AcceptRequest* request, AcceptResponse* response,
                       google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(base_cntl);
  if (FLAGS_simulate_network_error) {
    uint64_t sleep_time = butil::RandInt(0, 1200);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
  }

  std::string key = request->id().key();
  std::unique_lock<std::mutex> lk(mtx_for_storage_);
  auto it = storage_.find(key);
  if (it == storage_.end()) {
    Version ver;
    ver.last_bal = request->bal();
    ver.val = request->val();
    ver.v_bal = request->bal();
    storage_[key] = ver;
  } else {
    if (request->bal().seq() >= it->second.last_bal.seq()) {
      it->second.last_bal = request->bal();
      it->second.val = request->val();
      it->second.v_bal = request->bal();
    }
  }

  Ballot* last_bal = new Ballot(storage_[key].last_bal);
  response->set_allocated_last_bal(last_bal);
  LOG(INFO) << "server[" << node_id_ << "]receive Accept request from "
            << cntl->remote_side() << " to " << cntl->local_side()
            << ", request : " << request->ShortDebugString()
            << ", response: " << response->ShortDebugString();
}

int PaxosServer::Start() {
  brpc::ServerOptions options;
  PaxosImpl* paxos_impl = new PaxosImpl(node_id_);
  if (server_.AddService(paxos_impl, brpc::SERVER_OWNS_SERVICE) != 0) {
    LOG(ERROR) << "fail to add server[" << node_id_ << "]";
    return -1;
  }
  if (server_.Start(global_base_port + node_id_, &options) != 0) {
    LOG(ERROR) << "fail to start server[" << node_id_ << "]";
    return -1;
  }

  LOG(INFO) << "server[" << node_id_ << "] start success";
  return 0;
}

void PaxosServer::Stop() {
  server_.Stop(0);
  server_.Join();
}

std::string PaxosServer::GetServerAddr() const {
  return "127.0.0.1:" + std::to_string(global_base_port + node_id_);
}

Status PaxosServer::RunPaxos(uint64_t seq_num, const std::string& key,
                             const std::string& in_value,
                             std::string* out_value,
                             std::vector<PaxosServer*>* servers) {
  LOG(INFO) << "server[" << node_id_
            << "] recevie client request, begin paxos round";
  Status status = Status::kInit;
  uint64_t seq = seq_num;
  uint64_t next_seq;
  while (status != Status::kOK) {
    switch (status) {
      case Status::kInit:
        status = OncePaxos(seq, &next_seq, key, in_value, out_value, servers);
        break;

      case Status::kReject:
        seq = SetAndGetGlobalSeq(next_seq + 1);
        status = OncePaxos(seq, &next_seq, key, in_value, out_value, servers);
        break;

      case Status::kNotFound:
        return status;

      default:
        break;
    }
  }
  return status;
}

Status PaxosServer::OncePaxos(uint64_t in_seq, uint64_t* out_seq,
                              const std::string& key,
                              const std::string& in_value,
                              std::string* out_value,
                              std::vector<PaxosServer*>* servers) {
  LOG(INFO) << "server[" << node_id_ << "] use seq: " << in_seq
            << " begin a paxos round";
  int quorom = servers->size() / 2 + 1;
  int ok_requests = 0;
  uint64_t max_seq = 0;
  std::vector<PrepareResponse> prepare_result;
  PreparePhase(in_seq, key, servers, &prepare_result);
  Ballot max_val_bal;
  Value max_val;

  for (const auto& prepare_response : prepare_result) {
    if (prepare_response.last_bal().seq() > in_seq) {
      max_seq = std::max(max_seq, prepare_response.last_bal().seq());
      continue;
    }
    ok_requests++;
    if (max_val_bal.seq() < prepare_response.v_bal().seq()) {
      max_val_bal.set_seq(prepare_response.v_bal().seq());
      max_val.set_val(prepare_response.val().val());
    }
  }
  if (ok_requests < quorom) {
    *out_seq = max_val_bal.seq();
    LOG(WARNING) << std::this_thread::get_id()
                 << " prepare phase failed, try next seq";
    return Status::kReject;
  }

  if (in_value.empty()) {
    if (max_val.val().empty()) {
      return Status::kNotFound;
    }
    *out_value = max_val.val();
    return Status::kOK;
  }

  std::string accept_value = in_value;
  if (max_val_bal.seq() > 0) {
    accept_value = max_val.val();
    LOG(WARNING) << std::this_thread::get_id() << " confict with "
                 << max_val_bal.proposer_id() << ", use  " << accept_value
                 << " instead of " << in_value;
  }
  ok_requests = 0;
  max_seq = 0;
  std::vector<AcceptResponse> accept_result;
  AcceptPhase(in_seq, key, accept_value, servers, &accept_result);
  for (const auto& accept_response : accept_result) {
    if (accept_response.last_bal().seq() > in_seq) {
      max_seq = std::max(max_seq, accept_response.last_bal().seq());
      continue;
    }
    ok_requests++;
  }
  if (ok_requests < quorom) {
    *out_seq = max_val_bal.seq();
    LOG(WARNING) << std::this_thread::get_id()
                 << " accept phase failed, try next seq";
    return Status::kReject;
  }

  return Status::kOK;
}

void PaxosServer::PreparePhase(uint64_t seq_num, const std::string& key,
                               std::vector<PaxosServer*>* servers,
                               std::vector<PrepareResponse>* result) {
  for (PaxosServer* server : *servers) {
    PrepareRequest request;
    PaxosId* id = new PaxosId();
    id->set_key(key);
    request.set_allocated_id(id);
    Ballot* bal = new Ballot();
    bal->set_proposer_id(node_id_);
    bal->set_seq(seq_num);
    request.set_allocated_bal(bal);
    PrepareResponse response;

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.timeout_ms = 1000;
    if (channel.Init(server->GetServerAddr().c_str(), &options) != 0) {
      LOG(WARNING) << "channel init failed";
      continue;
    }
    Paxos_Stub stub(&channel);
    brpc::Controller cntl;
    stub.Prepare(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
      LOG(WARNING) << cntl.ErrorText();
      continue;
    }
    result->push_back(response);
  }
}

void PaxosServer::AcceptPhase(uint64_t seq_num, const std::string& key,
                              const std::string& in_value,
                              std::vector<PaxosServer*>* servers,
                              std::vector<AcceptResponse>* result) {
  for (PaxosServer* server : *servers) {
    AcceptRequest request;
    PaxosId* id = new PaxosId();
    id->set_key(key);
    request.set_allocated_id(id);
    Ballot* bal = new Ballot();
    bal->set_proposer_id(node_id_);
    bal->set_seq(seq_num);
    request.set_allocated_bal(bal);
    Value* val = new Value();
    val->set_val(in_value);
    request.set_allocated_val(val);
    AcceptResponse response;

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.timeout_ms = 1000;
    if (channel.Init(server->GetServerAddr().c_str(), &options) != 0) {
      LOG(WARNING) << "channel init failed";
      continue;
    }
    Paxos_Stub stub(&channel);
    brpc::Controller cntl;
    stub.Accept(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
      LOG(WARNING) << cntl.ErrorText();
      continue;
    }
    result->push_back(response);
  }
}

}  // namespace Paxos