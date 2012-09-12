-- | A reference/proof-of-concept implementation of @Eclair.Frontend@
--   in a simplified setting, using in-memory concurrent transactions.

{-# LANGUAGE TypeFamilies #-}
module Eclair.Backend.Reference where

import Control.Concurrent.STM
import Data.HashTable
import Data.Int
import Data.IntMap(IntMap)
import qualified Data.IntMap as IntMap
import Data.IORef
import Data.Unique
import Eclair.Frontend
import GHC.Prim


-- | Timestamps.
--   Under the assumption that the timestamps are large enough
--   so that an overflow does not occur.
type TS = Int

type family Payload o :: *
type family Update o :: *


-- | A versioned object is a mutable node in chains of
--   updates with a unique identity.
--   The parent(s) of a versioned object determine
--   its versions. 
--
--   VersionedObjects may not reference themselves.
--   Only spaces may refer to themselves.
data VersionedObject o
  = VersionedObject
      !( Unique )
      !( TVar (Node o) )  -- mutable node.

instance Eq (VersionedObject o) where
  (VersionedObject a _) == (VersionedObject b _) = a == b

instance Ord (VersionedObject o) where
  compare (VersionedObject a _) (VersionedObject b _) = compare a b

-- | A single version of a versioned object is a linear chain
--   of update-nodes, and all versions of a versioned object
--   are represented as an acyclic graph of update-nodes, where
--   the links between the nodes are created at particular points
--   in time.
--
--   The chains of versioned objects may need to be merged when
--   objects get assigned to spaces. If the chains end in different
--   base values, then merge nodes may need to be added. Depending
--   on the actual implementation, a merge node may simply be
--   considered as letting its payload win over its base.
--
--   A node value is not mutable: update the variable of the
--   VersionedObject instead.
--
--   An update may not be visible when it was issued be another
--   space that has not become an alias of the current space
--   yet.
data Node o
  = Join
      !( Payload o )
      !( Base o )
  | Update
      !( Update o )
      !( Base o )

-- | The base of an update: either it is unlinked, meaning that
--   it has not been assigned to a space yet, or it is linked
--   and the update may be involved in multiple chains.
--
--   Updates may be committed (global) or uncommitted (local).
--   Uncommitted updates may still have different base
--   versions when reading from an uncommitted snapshot.
--
--   Important invariant: any node having a base with history
--   h, has all versions smaller or equal to the version
--   in which the node occurs.
data Base o
  = None
  | Unlinked
      !( VersionedObject o )
  | Uncommitted
      -- the space where the parent belongs to
      !( RSpace o )

      -- versions of the children:
      -- these links exist in the same transaction, and are
      -- thus accessed from the same thread: we can use an
      -- IORef.
      !( IORef (Hist o) )
  | Committed
      -- the space where the parent belongs to
      !( RSpace o )
      -- versions of the children
      !( TVar (Hist o) )

-- | Ordered sequence of links to a base object.
type Hist o = IntMap (VersionedObject o)

-- | A space is gives a reference into the versioned
--   data graph, and allows versioned updates to be
--   applied to it.
--
--   To open a space at time t means to access all
--   updates <= t, in the set of aliasses <= t.
--
--   A space contains a reference to some state that
--   it may share with other spaces.
data RSpace o = RSpace
  !( Unique )
  !( RStore )
  !( TVar (SpaceShared o) )

instance Eq (RSpace o) where
  (RSpace a _ _) == (RSpace b _ _) = a == b

instance Ord (RSpace o) where
  compare (RSpace a _ _) (RSpace b _ _) = compare a b

instance Show (RSpace o) where
  show (RSpace u _ _) = "space#" ++ show (hashUnique u)

-- | From a given point in time, spaces may become
--   aliasses and share each updates, which is
--   accomplished by joining the history tables
--   and inserting a new entry from that point
--   in time that contains the join of the
--   most recent versions from that time.
--
--   A transaction may obtain exclusive ownership of
--   the history table of a space in order to perform
--   destructive updates.
data SpaceShared o = SpaceShared
  -- | The objects assigned to the space
  !( TMVar (Hist o) )

  -- | The aliasses (versioned as well)
  !( TVar (Aliasses o) )

-- | Ordered sequence of aliasses.
type Aliasses o = IntMap (RSpace o)


-- | The store.
data RStore = RStore
  { storeId    :: !Unique
  , storeClock :: !( TVar TS )
  }

instance Eq RStore where
  a == b = storeId a == storeId b

instance Ord RStore where
  compare a b = compare (storeId a) (storeId b)

instance Show RStore where
  show s = "store#" ++ (show $ hashUnique $ storeId s)

-- | A reference is actually just an alias for a space.
newtype RRef o = RRef (RSpace o)
  deriving (Eq, Ord, Show)


data RTrans = RTrans
  { txnStart   :: !Int
  , txnPublish :: !( HashTable AnyRef AnyObject )
  }

-- | This reference implementation probably does not need
--   to have a notion of a snapshot.
data RSnap = RSnap

instance IsStore RStore where
  type Ref RStore   = RRef
  type Trans RStore = RTrans
  type Snap RStore  = RSnap

  openTransaction   = undefined
  abortTransaction  = undefined
  commitTransaction = undefined

  createSnapshot  = undefined
  disposeSnapshot = undefined

  accessSpace = undefined
  allocSpace  = undefined
  updateSpace = undefined



-- | Initializes the store.
initStore :: IO RStore
initStore = do
  unq   <- newUnique
  clock <- newTVarIO 0
  return $ RStore
    { storeId    = unq
    , storeClock = clock
    }

-- | Increments the clock and returns the old value.
incrementClock :: RStore -> IO TS
incrementClock s = 
  let clock = storeClock s
  in atomically $ do
    t <- readTVar clock
    let t' = t + 1
    writeTVar clock $! t'
    return t


-- * Those pesky type parameters

type Whatever  = Any () -- can be coerced (unsafely) to any value
type AnyRef    = RRef Whatever
type AnyObject = VersionedObject Whatever

-- type SomeSpace  = InMemorySpace WhateverValue
-- type SomeRef    = InMemoryRef WhateverValue
-- type SomeObject = InMemoryObject WhateverValue
