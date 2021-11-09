module Dessin.Violeta.Timer
  ( resetElectionTimer
  , resetHeartbeatTimer
  , cancelTimer
  ) where

import Control.Lens hiding (Index)
import Control.Monad.Trans (lift)
import System.Random
import Control.Concurrent.Lifted

import Dessin.Violeta.Types
import Dessin.Violeta.Util

getNewElectionTimeout :: BftRaft nt et rt mt Int
getNewElectionTimeout = view (cfg.electionTimeoutRange) >>= lift . randomRIO

resetElectionTimer :: BftRaft nt et rt mt ()
resetElectionTimer = do
  timeout <- getNewElectionTimeout
  setTimedEvent (ElectionTimeout $ show (timeout `div` 1000) ++ "ms") timeout

resetHeartbeatTimer :: BftRaft nt et rt mt ()
resetHeartbeatTimer = do
  timeout <- view (cfg.heartbeatTimeout)
  setTimedEvent (HeartbeatTimeout $ show (timeout `div` 1000) ++ "ms") timeout

-- | Cancel any existing timer.
cancelTimer :: BftRaft nt et rt mt ()
cancelTimer = do
  use timerThread >>= maybe (return ()) killThread
  timerThread .= Nothing

-- | Cancels any pending timer and sets a new timer to trigger an event after t
-- microseconds.
setTimedEvent :: Event nt et rt -> Int -> BftRaft nt et rt mt ()
setTimedEvent e t = do
  cancelTimer
  tmr <- fork $ wait t >> enqueueEvent e
  timerThread .= Just tmr
