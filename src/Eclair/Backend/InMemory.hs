-- | A reference implementation of @Eclair.Frontend@ using
--   an in-memory database based on STM.

{-# LANGUAGE TypeFamilies #-}
module Eclair.Backend.InMemory where

import Control.Concurrent.STM
import Data.IntMap(IntMap)
import qualified Data.IntMap as IntMap
import Data.Unique
import Eclair.Frontend


{-

-- | The in-memory store contains a transaction log.
data InMemoryStore = InMemoryStore
  { -- should trim dead references regularly, e.g. after each transaction
    storeLog   :: !( TVar (IntMap (WeakRef TransHist)) )
  , -- global clock
    storeClock :: !( TVar Int )
  , -- the starting stamp of the oldest running transaction
    storeOldestRunning :: !(TVar Int)
  }

-- | An item in the transaction log, keeping track of the spaces that
--   were modified by a transaction, as well as the number of spaces
--   of which the version of the transaction is still current.
--
--   The count should be decremented when the transaction is no longer
--   current in a memory space, or when it is current when a space is
--   garbage collected.
--
--   When the count reaches 0, no space actively refers to objects
--   created by this transaction anymore, and the history record
--   can be eliminated when no transaction is running with a
--   starting timestamp smaller than the finish stamp of this
--   transaction.
data TransHist = TransHist
  { histFinishStamp :: !Int
  , histActiveCount :: !( TVar Int )
  , histSpaceLog    :: !( TVar [WeakRef SomeMemorySpace] )
  }

-- | The active transaction keeps track of the spaces it allocated
--   and the spaces it updates upon commit.
data InMemoryTrans = InMemoryTrans
  { transStartStamp :: !Int
  , transAllocs     :: !( IORef [SomeInMemoryRef] )
  , transWrites     :: !( IORef (Map SomeInMemoryRef SomeInMemoryObject) )
  }

-- | No special facility is needed for dealing with in-memory snapshots.
type InMemorySnap  = ()



type InMemoryRef = InMemorySpace
data InMemorySpace o
  = Space
      { spaceCurrent :: !( TVar (InMemoryObject o) )
      , spaceStamp   :: !( TVar Unique )
      , -- implementation will assume a collision-less mapping from unique to int
        spaceSnaps   :: !( TVar (IntMap (InMemoryObject o)) )
      }

data InMemoryObject o
  = Value
      { objValue :: o
      }
  | Redirect
      { objRedir :: !( TVar (InMemoryObject o) )
      }


-- * References to values of whatever type

type WhateverValue = () -- use Any data type
type SomeInMemorySpace  = InMemorySpace WhateverValue
type SomeInMemoryRef    = InMemoryRef WhateverValue
type SomeInMemoryObject = InMemoryObject WhateverValue




-- * The store implementation

instance IsStore InMemoryStore where
  type Trans InMemoryStore = InMemoryTrans
  type Snap InMemoryStore  = InMemorySnap
  type Ref InMemoryStore   = InMemoryRef

  openTransaction = undefined
  abortTransaction = undefined
  commitTransaction = undefined

  createSnapshot = undefined
  disposeSnapshot = undefined

  accessSpace = undefined
  allocSpace = undefined
  storeSpace = undefined

-}