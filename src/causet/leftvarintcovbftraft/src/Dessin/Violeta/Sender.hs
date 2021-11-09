module Dessin.Violeta.Sender
(   sendAppendEntries
  , sendAppendEntriesResponse
  , sendRequestVote
  , sendRequestVoteResponse
  , sendAllAppendEntries
  , sendAllRequestVotes
  , sendResults
  , sendRPC

	) where

	import Control.lens
	import Data.Foldable (traverse_)
	import Data.Sequence (Seq)
	import qualified Data.Sequence as Seq

	import Dessin.Violeta.Util
	import Dessin.Violeta.Types

	sendAppendEntries :: Ord nt => nt -> BftRaft nt et rt mt ()
	sendAppendEntries target = do
		mni <- use $ lNextIndex.at target
		es <- use logEntries
		let (pli, plt) = logInfoForNextIndex mni es
		ct <- use term
		nid <- view (cfg.nodeId)
		ci <- use commitIndex
		debug $ "sendAppendEntries: " ++ show ct
		sendRPC target $ AE $
			AppendEntries ct nid pli plt (Seq.drop (pli + 1) es) ci

	 sendAppendEntriesResponse :: nt -> Bool -> logIndex -> BftRaft nt et rt mt ()
	 sendAppendEntriesResponse target success lindex = do
	 	ct <- use term
	 	nid <- view(cfg.nodeId)
	 	es <- use logEntries
	 	let (llt, lli) = lastLogInfo es
	 	

-- called by leaders sending appendEntries.
-- given a replica's nextIndex, get the index and term to send as
-- prevLog(Index/Term)