-- | A reference implementation of @Eclair.Frontend@ using
--   an in-memory database based on STM.

{-# LANGUAGE TypeFamilies #-}
module Eclair.Backend.InMemory where

import Control.Concurrent.STM
import Data.Int
import Data.IntMap(IntMap)
import qualified Data.IntMap as IntMap
import Data.IORef
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



-- | Timestamps.
--   Under the assumption that the timestamps are large enough
--   so that an overflow does not occur.
type TS = Int

type family Payload o :: *
type family Update o :: *


-- | A versioned object is a mutable node in chains of
--   updates with a unique identity.
data VersionedObject o
  = VersionedObject
      !(TVar (Node o))  -- mutable node.

-- | A single version of a versioned object is a linear chain
--   of update-nodes, and all versions of a versioned object
--   are represented as a tree of update-nodes, where the
--   links between the nodes are created at particular points
--   in time.
--
--   The chains of versioned objects may need to be merged when
--   objects get assigned to spaces. If the chains end in different
--   base values, then merge nodes may need to be added. Depending
--   on the actual implementation, a merge node may simple be
--   considered as letting its payload win over its base.
--
--   A node value is not mutable: update the TVar of the
--   VersionedObject instead.
data Node o
  = Base
      ( Payload o )
  | Merge
      ( Payload o )
      !( VersionedObject o )
  | Update
      { voUpdate :: !( Update o )
      , voOrigin :: !( Base o )
      }

-- | The base of an update: either it is unlinked, meaning that
--   it has not been assigned to a space yet, or it is linked
--   and the update may be involved in multiple chains.
--
--   Updates may be committed (global) or uncommitted (local).
--   Uncommitted updates may still have different base
--   versions when reading from an uncommitted snapshot.
data Base o
  = Unlinked
      !( VersionedObject o )
  | Uncommitted
      -- these links exist in the same transaction, and are
      -- thus accessed from the same thread: we can use an
      -- IORef.
      !( IORef (Hist o) )
  | Committed
      !( TVar (Hist o) )

-- | Ordered sequence of links to a base object.
type Hist o = IntMap (VersionedObject o)
