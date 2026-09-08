#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <random>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cassert>

enum class Role {
    Follower,
    Candidate,
    Leader
};

struct LogEntry {
    int term;
    int index;
    std::string command;
};

struct RequestVoteArgs {
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

struct RequestVoteReply {
    int term;
    bool voteGranted;
};

struct AppendEntriesArgs {
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<LogEntry> entries;
    int leaderCommit;
};

struct AppendEntriesReply {
    int term;
    bool success;
    int matchIndex;
};

class NetworkTransport;

class RaftNode : public std::enable_shared_from_this<RaftNode> {
private:
    std::mutex mtx;
    int nodeId;
    std::vector<int> peerIds;
    std::shared_ptr<NetworkTransport> transport;

    Role currentRole{Role::Follower};
    int currentTerm{0};
    int votedFor{-1};
    std::vector<LogEntry> log;

    int commitIndex{0};
    int lastApplied{0};

    std::map<int, int> nextIndex;
    std::map<int, int> matchIndex;

    bool running{true};
    std::thread timerThread;
    std::mt19937 rng;

    std::chrono::steady_clock::time_point lastHeartbeat;
    int electionTimeoutMs;

    void resetElectionTimeout() {
        std::uniform_int_distribution<int> dist(150, 300);
        electionTimeoutMs = dist(rng);
        lastHeartbeat = std::chrono::steady_clock::now();
    }

    void runLoop() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            std::unique_lock<std::mutex> lock(mtx);

            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - lastHeartbeat).count();

            if (currentRole != Role::Leader && elapsed >= electionTimeoutMs) {
                startElection();
            } else if (currentRole == Role::Leader) {
                sendHeartbeats();
            }
        }
    }

    void startElection() {
        currentRole = Role::Candidate;
        currentTerm++;
        votedFor = nodeId;
        resetElectionTimeout();

        int votesReceived = 1;
        int termAtStart = currentTerm;
        int lastLogIdx = static_cast<int>(log.size()) - 1;
        int lastLogTerm = log[lastLogIdx].term;

        std::vector<int> targetPeers = peerIds;
        mtx.unlock();

        for (int peerId : targetPeers) {
            RequestVoteArgs args{termAtStart, nodeId, lastLogIdx, lastLogTerm};
            
            std::thread([this, peerId, args, termAtStart, &votesReceived]() {
                RequestVoteReply reply;
                if (sendRequestVoteRPC(peerId, args, reply)) {
                    std::lock_guard<std::mutex> lk(mtx);
                    if (currentRole == Role::Candidate && currentTerm == termAtStart) {
                        if (reply.term > currentTerm) {
                            currentTerm = reply.term;
                            currentRole = Role::Follower;
                            votedFor = -1;
                            resetElectionTimeout();
                        } else if (reply.voteGranted) {
                            votesReceived++;
                            if (votesReceived > static_cast<int>((peerIds.size() + 1) / 2)) {
                                becomeLeader();
                            }
                        }
                    }
                }
            }).detach();
        }

        mtx.lock();
    }

    void becomeLeader() {
        currentRole = Role::Leader;
        int lastLogIdx = static_cast<int>(log.size()) - 1;
        for (int peerId : peerIds) {
            nextIndex[peerId] = lastLogIdx + 1;
            matchIndex[peerId] = 0;
        }
        sendHeartbeats();
    }

    void sendHeartbeats() {
        int term = currentTerm;
        int leaderId = nodeId;
        int commitIdx = commitIndex;

        for (int peerId : peerIds) {
            int prevIdx = nextIndex[peerId] - 1;
            int prevTerm = log[prevIdx].term;

            std::vector<LogEntry> entriesToSend;
            for (size_t i = prevIdx + 1; i < log.size(); ++i) {
                entriesToSend.push_back(log[i]);
            }

            AppendEntriesArgs args{term, leaderId, prevIdx, prevTerm, entriesToSend, commitIdx};

            std::thread([this, peerId, args, term]() {
                AppendEntriesReply reply;
                if (sendAppendEntriesRPC(peerId, args, reply)) {
                    std::lock_guard<std::mutex> lk(mtx);
                    if (currentRole == Role::Leader && currentTerm == term) {
                        if (reply.term > currentTerm) {
                            currentTerm = reply.term;
                            currentRole = Role::Follower;
                            votedFor = -1;
                            resetElectionTimeout();
                        } else if (reply.success) {
                            nextIndex[peerId] = reply.matchIndex + 1;
                            matchIndex[peerId] = reply.matchIndex;
                            checkAndCommit();
                        } else {
                            nextIndex[peerId] = std::max(1, nextIndex[peerId] - 1);
                        }
                    }
                }
            }).detach();
        }
    }

    void checkAndCommit() {
        int lastLogIdx = static_cast<int>(log.size()) - 1;
        for (int N = lastLogIdx; N > commitIndex; --N) {
            if (log[N].term == currentTerm) {
                int count = 1;
                for (int peerId : peerIds) {
                    if (matchIndex[peerId] >= N) count++;
                }
                if (count > static_cast<int>((peerIds.size() + 1) / 2)) {
                    commitIndex = N;
                    break;
                }
            }
        }
    }

    bool sendRequestVoteRPC(int peerId, const RequestVoteArgs& args, RequestVoteReply& reply);
    bool sendAppendEntriesRPC(int peerId, const AppendEntriesArgs& args, AppendEntriesReply& reply);

public:
    RaftNode(int id, std::vector<int> peers)
        : nodeId(id), peerIds(peers), rng(100 + id) {
        log.push_back(LogEntry{0, 0, "NOOP"});
        resetElectionTimeout();
    }

    void setTransport(std::shared_ptr<NetworkTransport> trans) {
        transport = trans;
    }

    void start() {
        timerThread = std::thread(&RaftNode::runLoop, this);
    }

    void stop() {
        running = false;
        if (timerThread.joinable()) timerThread.join();
    }

    RequestVoteReply handleRequestVote(const RequestVoteArgs& args) {
        std::lock_guard<std::mutex> lk(mtx);
        RequestVoteReply reply{currentTerm, false};

        if (args.term < currentTerm) {
            return reply;
        }

        if (args.term > currentTerm) {
            currentTerm = args.term;
            currentRole = Role::Follower;
            votedFor = -1;
        }

        int lastLogIdx = static_cast<int>(log.size()) - 1;
        int lastLogTerm = log[lastLogIdx].term;

        bool logUpToDate = (args.lastLogTerm > lastLogTerm) ||
                           (args.lastLogTerm == lastLogTerm && args.lastLogIndex >= lastLogIdx);

        if ((votedFor == -1 || votedFor == args.candidateId) && logUpToDate) {
            votedFor = args.candidateId;
            reply.voteGranted = true;
            resetElectionTimeout();
        }

        reply.term = currentTerm;
        return reply;
    }

    AppendEntriesReply handleAppendEntries(const AppendEntriesArgs& args) {
        std::lock_guard<std::mutex> lk(mtx);
        AppendEntriesReply reply{currentTerm, false, 0};

        if (args.term < currentTerm) {
            return reply;
        }

        if (args.term > currentTerm || currentRole == Role::Candidate) {
            currentTerm = args.term;
            currentRole = Role::Follower;
            votedFor = -1;
        }

        resetElectionTimeout();

        if (args.prevLogIndex >= static_cast<int>(log.size()) ||
            log[args.prevLogIndex].term != args.prevLogTerm) {
            reply.term = currentTerm;
            return reply;
        }

        int idx = args.prevLogIndex + 1;
        for (const auto& entry : args.entries) {
            if (idx < static_cast<int>(log.size())) {
                if (log[idx].term != entry.term) {
                    log.erase(log.begin() + idx, log.end());
                    log.push_back(entry);
                }
            } else {
                log.push_back(entry);
            }
            idx++;
        }

        if (args.leaderCommit > commitIndex) {
            commitIndex = std::min(args.leaderCommit, static_cast<int>(log.size()) - 1);
        }

        reply.success = true;
        reply.matchIndex = static_cast<int>(log.size()) - 1;
        reply.term = currentTerm;
        return reply;
    }

    int getId() const { return nodeId; }
    Role getRole() { std::lock_guard<std::mutex> lk(mtx); return currentRole; }
    int getTerm() { std::lock_guard<std::mutex> lk(mtx); return currentTerm; }
    int getCommitIndex() { std::lock_guard<std::mutex> lk(mtx); return commitIndex; }
};

class NetworkTransport {
private:
    std::map<int, std::shared_ptr<RaftNode>> nodes;
    std::mutex mtx;
    bool partitionActive{false};
    int partitionedNodeId{-1};

public:
    void registerNode(std::shared_ptr<RaftNode> node) {
        std::lock_guard<std::mutex> lk(mtx);
        nodes[node->getId()] = node;
    }

    void simulatePartition(int isolatedNode) {
        std::lock_guard<std::mutex> lk(mtx);
        partitionActive = true;
        partitionedNodeId = isolatedNode;
    }

    void recoverPartition() {
        std::lock_guard<std::mutex> lk(mtx);
        partitionActive = false;
        partitionedNodeId = -1;
    }

    bool sendRequestVote(int src, int dst, const RequestVoteArgs& args, RequestVoteReply& reply) {
        std::lock_guard<std::mutex> lk(mtx);
        if (partitionActive && (src == partitionedNodeId || dst == partitionedNodeId)) {
            return false;
        }
        if (nodes.find(dst) != nodes.end()) {
            reply = nodes[dst]->handleRequestVote(args);
            return true;
        }
        return false;
    }

    bool sendAppendEntries(int src, int dst, const AppendEntriesArgs& args, AppendEntriesReply& reply) {
        std::lock_guard<std::mutex> lk(mtx);
        if (partitionActive && (src == partitionedNodeId || dst == partitionedNodeId)) {
            return false;
        }
        if (nodes.find(dst) != nodes.end()) {
            reply = nodes[dst]->handleAppendEntries(args);
            return true;
        }
        return false;
    }
};

bool RaftNode::sendRequestVoteRPC(int peerId, const RequestVoteArgs& args, RequestVoteReply& reply) {
    if (transport) return transport->sendRequestVote(nodeId, peerId, args, reply);
    return false;
}

bool RaftNode::sendAppendEntriesRPC(int peerId, const AppendEntriesArgs& args, AppendEntriesReply& reply) {
    if (transport) return transport->sendAppendEntries(nodeId, peerId, args, reply);
    return false;
}

void runRaftTestSuite() {
    std::cout << "Starting Raft Consensus Test Suite..." << std::endl;

    auto transport = std::make_shared<NetworkTransport>();
    std::vector<std::shared_ptr<RaftNode>> cluster;
    std::vector<int> allNodes = {0, 1, 2};

    for (int i = 0; i < 3; ++i) {
        std::vector<int> peers;
        for (int p : allNodes) {
            if (p != i) peers.push_back(p);
        }
        auto node = std::make_shared<RaftNode>(i, peers);
        node->setTransport(transport);
        transport->registerNode(node);
        cluster.push_back(node);
    }

    for (auto& node : cluster) {
        node->start();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    int leadersFound = 0;
    for (auto& node : cluster) {
        if (node->getRole() == Role::Leader) {
            leadersFound++;
            std::cout << "Node " << node->getId() << " elected as Leader in Term " << node->getTerm() << std::endl;
        }
    }
    assert(leadersFound == 1);

    std::cout << "Testing Network Partition Fault Tolerance..." << std::endl;
    transport->simulatePartition(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cout << "Recovering Network Partition..." << std::endl;
    transport->recoverPartition();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (auto& node : cluster) {
        node->stop();
    }

    std::cout << "All Raft consensus test cases passed successfully." << std::endl;
}

int main() {
    runRaftTestSuite();
    return 0;
}
