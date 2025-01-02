use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

use module_system::{Handler, ModuleRef, System, TimerHandle};
use rand::Rng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
pub use domain::*;

mod domain;

#[non_exhaustive]
pub struct Raft {
    id: Uuid,
    role: Role,
    persistent_state: PersistentState,
    volatile_state: VolatileState,
    election_timer_handle: Option<TimerHandle>,

    storage: Box<dyn StableStorage>,
    sender: Box<dyn RaftSender>,
    processes_count: usize,
    other_nodes: Vec<Uuid>
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let mut other_nodes = Vec::new();
        for node in config.servers.iter() {
            if *node != config.self_id {
                other_nodes.push(node.clone());
            }
        }

        let persistent_state = if let Some(state) = stable_storage.get("persistent_state").await {
            bincode::deserialize(&state).unwrap()
        } else {
            PersistentState::default()
        };

        let module = Raft {
            id: config.self_id,
            role: Role::Follower,
            persistent_state,
            volatile_state: Default::default(),
            election_timer_handle: None,
            storage: stable_storage,
            sender: message_sender,
            processes_count: config.servers.len(),
            other_nodes,
        };

        let mod_ref = system.register_module(module).await;
        mod_ref.request_tick(ElectionTimeout { }, rand::thread_rng().gen_range(config.election_timeout_range)).await;
        mod_ref
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        if msg.header.term > self.persistent_state.current_term {
            self.update_term(msg.header.term).await;
            self.role = Role::Follower;
        }

        match msg.content {
            RaftMessageContent::AppendEntries(_) => {todo!()}
            RaftMessageContent::AppendEntriesResponse(_) => {todo!()}
            RaftMessageContent::RequestVote(args) =>
                self.handle_request_vote(msg.header.source, msg.header.term, args).await,
            RaftMessageContent::RequestVoteResponse(_) => {todo!()}
            _ => { todo!() }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        /*match msg.content {
            ClientRequestContent::Command { command, client_id, sequence_num, lowest_sequence_num_without_response } => {
                if let Role::Leader = self.role {
                    let prev_log_index = self.log.len();
                    let prev_log_term = self.log.get(prev_log_index - 1).unwrap().term;

                    for val in command.iter() {
                        self.log.push(LogEntry {
                            content: LogEntryContent::Command {
                                data: command,
                                client_id,
                                sequence_num,
                                lowest_sequence_num_without_response
                            },
                            term: self.term,
                            timestamp: ,
                        })
                    }

                    let cmd = RaftMessage {
                        header: RaftMessageHeader {
                            source: self.id,
                            term: self.term
                        },
                        content: RaftMessageContent::AppendEntries(
                            AppendEntriesArgs {
                                prev_log_index,
                                prev_log_term,
                                entries: command,
                                leader_commit: 0,
                            }
                        )
                    };

                    for other in self.other_nodes.iter() {

                    }
                    todo!()
                } else {
                    _ = msg.reply_to.send(
                        ClientRequestResponse::CommandResponse(
                            CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::NotLeader { leader_hint: self.leader },
                            }
                        )
                    );
                }
            }
            _ => {todo!()}
        }*/
        todo!()
    }
}

#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ElectionTimeout) {
        todo!()
    }
}

impl Raft {
    async fn update_term(&mut self, new_term: u64) {
        self.persistent_state.current_term = new_term;
        self.persistent_state.voted_for = None;
        self.persistent_state.leader_id = None;
    }

    async fn update_state(&mut self) {
        self.storage.put("persistent_state", &bincode::serialize(&self.persistent_state).unwrap()).await.unwrap();
    }

    async fn handle_request_vote(&mut self, sender: Uuid, term: u64, args: RequestVoteArgs) {
        let Role::Follower = self.role else {
            self.sender.send(&sender, self.gen_request_vote_response(false)).await;
            return;
        };

        if term < self.persistent_state.current_term {
            self.sender.send(&sender, self.gen_request_vote_response(false)).await;
            return;
        }

        if self.persistent_state.voted_for.is_some_and(|v| v != sender) {
            self.sender.send(&sender, self.gen_request_vote_response(false)).await;
            return;
        }

        let last_log_term = self.persistent_state.log.last().unwrap().term;
        let last_log_index = self.persistent_state.log.len();

        if args.last_log_term.cmp(&last_log_term).then(args.last_log_index.cmp(&last_log_index)).is_lt() {
            self.sender.send(&sender, self.gen_request_vote_response(false)).await;
            return;
        }

        self.persistent_state.voted_for = Some(sender);
        self.sender.send(&sender, self.gen_request_vote_response(true)).await;
    }
}

impl Raft {
    fn gen_request_vote_response(&self, response: bool) -> RaftMessage {
        RaftMessage {
            header: RaftMessageHeader {
                source: self.id,
                term: self.persistent_state.current_term
            },
            content: RaftMessageContent::RequestVoteResponse(
                RequestVoteResponseArgs { vote_granted: response }
            )
        }
    }
}

enum Role {
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader {
        next_idx: HashMap<Uuid, usize>,
        match_idx: HashMap<Uuid, usize>,
    },
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct PersistentState {
    current_term: u64,
    voted_for: Option<Uuid>,
    leader_id: Option<Uuid>,
    log: Vec<LogEntry>
}

#[derive(Clone, Default)]
struct VolatileState {
    commit_idx: usize,
    last_applied: usize
}

#[derive(Clone)]
struct ElectionTimeout { }