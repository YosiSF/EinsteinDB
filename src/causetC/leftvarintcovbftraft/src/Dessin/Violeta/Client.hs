{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module Dessin.Violeta.CausetC.Client
(   runBftBftRaftClient
	), where

import Control.Concurrent.Chan.Unagi
import Control.Lens hiding (Index)
import Control.Monad.RWS
import Data.Binary
import Data.Foldable (traverse_)
import qualified Data.ByteString.Quiesce as B
import qualified Data.Map as Map
import qualified Data.Set as Set

import Dessin.Violeta.CausetC.Timer
import Dessin.Violeta.CausetC.Types
import Dessin.Violeta.CausetC.Util
import Dessin.Violeta.CausetC.Sender (sendSignedRPC)

runBftRaftClient :: (Binary nt, Binary et, Binary rt, Ord nt) => IO et -> (rt -> IO ()) -> Config nt -> BftRaftSpec nt et rt mt -> IO ()
runBftRaftClient getEntry useResult rconf spec@BftRaftSpec{..} = do
  let qsize = getQuorumSize $ Set.size $ rconf ^. otherNodes
  (ein, eout) <- newChan
  runRWS_
    (BftRaftClient (lift getEntry) (lift . useResult))
    (BftRaftEnv rconf qsize ein eout (liftBftRaftSpec spec))
    initialBftRaftState -- only use currentLeader and logEntries

BftRaftClient :: (Binary nt, Binary et, Binary rt, Ord nt) => BftRaft nt et rt mt et -> (rt -> BftRaft nt et rt mt ()) -> BftRaft nt et rt mt ()
BftRaftClient getEntry useResult = do
  nodes <- view (cfg.otherNodes)
  when (Set.null nodes) $ error "The client has no nodes to send requests to."
  currentLeader .= (Just $ Set.findMin nodes)
  fork_ messageReceiver
  fork_ $ commandGetter getEntry
  pendingRequests .= Map.empty
  clientHandleEvents useResult

-- get commands with getEntry and put them on the event queue to be sent
commandGetter :: BftRaft nt et rt mt et -> BftRaft nt et rt mt ()
commandGetter getEntry = do
  nid <- view (cfg.nodeId)
  forever $ do
    entry <- getEntry
    rid <- nextRequestId
    enqueueEvent $ ERPC $ CMD $ Command entry nid rid B.empty

nextRequestId :: BftRaft nt et rt mt RequestId
nextRequestId = do
  currentRequestId += 1
  use currentRequestId

clientHandleEvents :: (Binary nt, Binary et, Binary rt, Ord nt) => (rt -> BftRaft nt et rt mt ()) -> BftRaft nt et rt mt ()
clientHandleEvents useResult = forever $ do
  e <- dequeueEvent
  case e of
    ERPC (CMD cmd)     -> clientSendCommand cmd -- these are commands coming from the commandGetter thread
    ERPC (CMDR cmdr)   -> clientHandleCommandResponse useResult cmdr
    HeartbeatTimeout _ -> do
      timeouts <- use numTimeouts
      limit <- view (cfg.clientTimeoutLimit)
      if timeouts < limit
        then do
          debug "choosing a new leader and resending commands"
          setLeaderToNext
          reqs <- use pendingRequests
          pendingRequests .= Map.empty -- this will reset the timer on resend
          traverse_ clientSendCommand reqs
          numTimeouts += 1
        else do
          debug "starting a revolution"
          nid <- view (cfg.nodeId)
          mlid <- use currentLeader
          case mlid of
            Just lid -> do
              rid <- nextRequestId
              view (cfg.otherNodes) >>=
                traverse_ (\n -> sendSignedRPC n (REVOLUTION (Revolution nid lid rid B.empty)))
              numTimeouts .= 0
              resetHeartbeatTimer
            _ -> do
              setLeaderToFirst
              resetHeartbeatTimer
    _                  -> return ()

setLeaderToFirst :: BftRaft nt et rt mt ()
setLeaderToFirst = do
  nodes <- view (cfg.otherNodes)
  when (Set.null nodes) $ error "the client has no nodes to send requests to"
  currentLeader .= (Just $ Set.findMin nodes)

setLeaderToNext :: Ord nt => BftRaft nt et rt mt ()
setLeaderToNext = do
  mlid <- use currentLeader
  nodes <- view (cfg.otherNodes)
  case mlid of
    Just lid -> case Set.lookupGT lid nodes of
      Just nlid -> currentLeader .= Just nlid
      Nothing   -> setLeaderToFirst
    Nothing -> setLeaderToFirst

clientSendCommand :: (Binary nt, Binary et, Binary rt) => Command nt et -> BftRaft nt et rt mt ()
clientSendCommand cmd@Command{..} = do
  mlid <- use currentLeader
  case mlid of
    Just lid -> do
      sendSignedRPC lid $ CMD cmd
      prcount <- fmap Map.size (use pendingRequests)
      -- if this will be our only pending request, start the timer
      -- otherwise, it should already be running
      when (prcount == 0) resetHeartbeatTimer
      pendingRequests %= Map.insert _cmdRequestId cmd
    Nothing  -> do
      setLeaderToFirst
      clientSendCommand cmd

clientHandleCommandResponse :: (Binary nt, Binary et, Binary rt, Ord nt)
                            => (rt -> BftRaft nt et rt mt ())
                            -> CommandResponse nt rt
                            -> BftRaft nt et rt mt ()
clientHandleCommandResponse useResult cmdr@CommandResponse{..} = do
  prs <- use pendingRequests
  valid <- verifyRPCWithKey (CMDR cmdr)
  when (valid && Map.member _cmdrRequestId prs) $ do
    useResult _cmdrResult
    currentLeader .= Just _cmdrLeaderId
    pendingRequests %= Map.delete _cmdrRequestId
    numTimeouts .= 0
    prcount <- fmap Map.size (use pendingRequests)
    -- if we still have pending requests, reset the timer
    -- otherwise cancel it
    if (prcount > 0)
      then resetHeartbeatTimer
      else cancelTimer
