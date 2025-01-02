use std::collections::HashMap;
use std::time::SystemTime;

use module_system::{Handler, ModuleRef, System};
use uuid::Uuid;
pub use domain::*;

mod domain;

#[non_exhaustive]
pub struct Raft {
    id: Uuid,
    term: u64,
    role: Role,
    leader: Option<Uuid>,
    other_nodes: Vec<Uuid>,
    voted_for: Option<Uuid>,
    log: Vec<LogEntry>,
    commit_idx: usize,
    last_applied: usize,
    next_idx: HashMap<Uuid, usize>,
    match_idx: HashMap<Uuid, usize>,
    message_sender: Box<dyn RaftSender>,
    stable_storage: Box<dyn StableStorage>,
    state_machine: Box<dyn StateMachine>,
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
        for node in config.servers {
            if node != config.self_id {
                other_nodes.push(node);
            }
        }

        let module = Self {
            id: config.self_id,
            term: 0,
            role: Role::Follower,
            leader: None,
            other_nodes,
            voted_for: None,
            log: vec![],
            commit_idx: 0,
            last_applied: 0,
            next_idx: Default::default(),
            match_idx: Default::default(),
            message_sender,
            stable_storage,
            state_machine,
        };

        system.register_module(module).await
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        match msg.content {
            RaftMessageContent::AppendEntries(_) => {todo!()}
            RaftMessageContent::AppendEntriesResponse(_) => {todo!()}
            RaftMessageContent::RequestVote(_) => {todo!()}
            RaftMessageContent::RequestVoteResponse(_) => {todo!()}
            _ => { todo!() }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        match msg.content {
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
        }
    }
}

enum Role {
    Follower,
    Candidate,
    Leader
}