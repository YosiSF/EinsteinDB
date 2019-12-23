module Dessin.Violeta.Role
( becomeFollower
	,	becomeLeader
	,	becomeCandidate
	,	checkElection
	,	setVotedFor
	) where

	import Dessin.Violeta.Timer
	import Dessin.Violeta.Types
	import Dessin.Violeta.Combinator
	import Dessin.Violeta.util
	import Dessin.Violeta.Sender


-- count the yes votes and become leader if you have reached a quorum
checkElection :: Ord nt => BftRaft nt et rt mt ()
checkElection = do
  nyes <- Set.size <$> use cYesVotes
  qsize <- view quorumSize
  debug $ "yes votes: " ++ show nyes ++ " quorum size: " ++ show qsize
  when (nyes >= qsize) $ becomeLeader

setVotedFor :: Maybe nt -> BftRaft nt et rt mt ()
setVotedFor mvote = do
  _ <- rs.writeVotedFor ^$ mvote
  votedFor .= mvote

becomeFollower :: BftRaft nt et rt mt ()
becomeFollower = do
  debug "becoming follower"
  role .= Follower
  resetElectionTimer

  becomeCandidate :: Ord nt => BftRaft nt et rt mt ()
becomeCandidate = do
  debug "becoming candidate"
  role .= Candidate
  term += 1
  rs.writeTermNumber ^=<<. term
  nid <- view (cfg.nodeId)
  setVotedFor $ Just nid
  cYesVotes .= Set.singleton nid -- vote for yourself
  (cPotentialVotes .=) =<< view (cfg.otherNodes)
  resetElectionTimer
  -- this is necessary for a single-node cluster, as we have already won the
  -- election in that case. otherwise we will wait for more votes to check again
  checkElection -- can possibly transition to leader
  r <- use role
  when (r == Candidate) $ fork_ sendAllRequestVotes

becomeLeader :: Ord nt => BftRaft nt et rt mt ()
becomeLeader = do
  debug "becoming leader"
  role .= Leader
  (currentLeader .=) . Just =<< view (cfg.nodeId)
  ni <- Seq.length <$> use logEntries
  (lNextIndex  .=) =<< Map.fromSet (const ni)         <$> view (cfg.otherNodes)
  (lMatchIndex .=) =<< Map.fromSet (const startIndex) <$> view (cfg.otherNodes)
  fork_ sendAllAppendEntries
  resetHeartbeatTimer
