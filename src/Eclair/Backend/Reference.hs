-- | A reference/proof-of-concept implementation of @Eclair.Frontend@
--   in a simplified setting, using in-memory concurrent transactions.

{-# LANGUAGE TypeFamilies, GADTs #-}
module Eclair.Backend.Reference where

import Control.Concurrent.STM
import Control.Monad
import Data.HashTable(HashTable)
import qualified Data.HashTable as HashTable
import Data.Int
import Data.IntMap(IntMap)
import qualified Data.IntMap as IntMap
import Data.IORef
import Data.Set(Set)
import qualified Data.Set as Set
import Data.Unique
import Eclair.Frontend
import Unsafe.Coerce


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

instance IsObj (VersionedObject o) where
  type ObjStore (VersionedObject o) = RStore
  type ObjType (VersionedObject o)  = Payload o

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
  | Committed
      -- the space in which the parent node has been created
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
  !( TVar (SpaceShared o) )

instance Eq (RSpace o) where
  (RSpace a _) == (RSpace b _) = a == b

instance Ord (RSpace o) where
  compare (RSpace a _) (RSpace b _) = compare a b

instance Show (RSpace o) where
  show (RSpace u _) = "space#" ++ show (hashUnique u)

-- | From a given point in time, spaces may become
--   aliased and share updates, which is
--   accomplished by joining the history tables
--   and inserting a new entry from that point
--   in time that contains the join of the
--   most recent versions from that time.
--
--   A transaction may obtain exclusive ownership of
--   the history table of a space in order to perform
--   destructive updates.
--
--   A space may be committed or uncommitted. In the
--   uncommitted state, it can only be accessed by
--   a reference from a single transaction.
data SpaceShared o
  = SpaceCommitted
      -- | The objects assigned to the space
      !( TMVar (Hist o) )

      -- | The aliasses (versioned as well)
      !( TVar (Aliasses o) )
  | SpaceUncommitted
      !( VersionedObject o ) 

-- | Ordered sequence of aliasses.
type Aliasses o = IntMap (Set (RSpace o))


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
data RRef :: * -> * where
  RRef :: RSpace o -> RRef (VersionedObject o)

instance Eq (RRef o) where
  (RRef a) == (RRef b) = a == b

instance Ord (RRef o) where
  compare (RRef a) (RRef b) = compare a b

instance Show (RRef o) where
  show (RRef a) = "ref:" ++ show a

hashRRef :: RRef o -> Int32
hashRRef (RRef (RSpace u _)) = fromIntegral $ hashUnique u 

data RTrans = RTrans
  { txnStart   :: !Int
  , txnPublish :: !( HashTable AnyRef RootObject )
  }

-- | A root object of a space, of which the actual type is hidden (existential).
data RootObject :: * where
  SomeRoot :: (vo ~ VersionedObject o, IsRoot vo) => !vo -> RootObject

-- | This reference implementation probably does not need
--   to have a notion of a snapshot.
data RSnap = RSnap

instance IsStore RStore where
  type Ref RStore   = RRef
  type Trans RStore = RTrans
  type Snap RStore  = RSnap

  openTransaction  s = do 
    ts <- readClock s
    updates <- HashTable.new (==) hashRRef
    return $ RTrans {txnStart = ts, txnPublish = updates}

  abortTransaction _ _ = return ()
  commitTransaction = undefined

  createSnapshot  _ _ _ = return RSnap
  disposeSnapshot _ _ _ = return ()

 
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
    return t'

-- | Increments the clock and returns the old value.
readClock :: RStore -> IO TS
readClock s = 
  let clock = storeClock s
  in atomically $ do
    t <- readTVar clock
    return t


instance IsRoot (VersionedObject o) where

  accessSpace s txn snap ref output = do
    -- find a locally updated version, if any.
    mbLocal <- lookupLocal txn ref
    case mbLocal of
      Just obj -> output obj
      Nothing  ->
        -- get the appropriate global version
        case ref of
          RRef space -> lookupSpace txn space >>= output

  allocSpace s txn _ obj output = do
    spaceVar <- newTVarIO $! SpaceUncommitted obj
    u <- newUnique
    let space = RSpace u spaceVar
        ref   = RRef $! space
    registerPublish txn ref obj 
    output ref

  updateSpace s txn _ ref obj output = do
    registerPublish txn ref obj
    output ()

-- | The history must contain a mapping with
--   a key smaller or equal to the @ts@.
histLookup :: TS -> IntMap t -> t
histLookup ts hist = 
  case IntMap.splitLookup ts hist of
    (smaller, mbObj, _) ->
      case mbObj of
        Nothing  -> snd $ IntMap.findMax smaller
        Just obj -> obj

-- | Finds the locally published contents of a space, if any.
lookupLocal :: RTrans -> RRef (VersionedObject o) -> IO (Maybe (VersionedObject o))
lookupLocal txn ref = do
  let published = txnPublish txn
      key       = unsafeCoerce ref
  mbRoot <- HashTable.lookup key published
  case mbRoot of
    Nothing             -> return Nothing
    Just (SomeRoot obj) -> return $! unsafeCoerce obj

-- | Finds the object in the space as observed by the given transaction.
lookupSpace :: RTrans -> RSpace o -> IO (VersionedObject o)
lookupSpace txn (RSpace _ sharedVar) = do
  content <- atomically $ readTVar sharedVar
  case content of
    SpaceCommitted histVar _ -> do
      hist <- atomically $ readTMVar histVar
      let ts = txnStart txn
      return $! histLookup ts hist 
    SpaceUncommitted obj -> return obj

-- | Registers the mapping from reference to object with the transaction.
registerPublish :: RTrans -> RRef (VersionedObject o) -> VersionedObject o -> IO ()
registerPublish txn ref obj = do
  let tbl = txnPublish txn
      key = unsafeCoerce ref
      val = SomeRoot obj
  void $ HashTable.update tbl key val

-- * Those pesky type parameters

type Whatever  = () -- can be coerced (unsafely) to any value
type AnyRef    = RRef Whatever
type AnyObject = VersionedObject Whatever

-- type SomeSpace  = InMemorySpace WhateverValue
-- type SomeRef    = InMemoryRef WhateverValue
-- type SomeObject = InMemoryObject WhateverValue


-- * Traversal of updates

buildUpdates ::
  TS -> RSpace o ->
  (Either (Payload o) (Update o) -> Payload o -> IO (Payload o)) ->
  VersionedObject o -> IO (Payload o)
buildUpdates ts accessSpace f obj = foldObject obj where
  foldObject (VersionedObject _ nodeVar) = do
    node <- atomically $ readTVar nodeVar
    case node of
      Join payload None  -> return payload
      Join payload base  -> foldBase (f $ Left payload) base
      Update update base -> foldBase (f $ Right update) base

  foldBase g base = do
    case base of
      None         -> error "foldBase: encountered a NONE value"
      Unlinked obj -> foldObject obj >>= g
      Committed creationSpace histVar -> do
        visible <- isVisible ts creationSpace accessSpace
        hist <- atomically $ readTVar histVar
        let obj = histLookup ts hist
        payload <- foldObject obj
        if visible
         then g payload
         else return payload

-- | folds over the updates from most-recent to older,
--   where returning @Right b@ causes the scan to stop
--   with the result @b@.
foldUpdates ::
  TS -> RSpace o ->
  (Either (Payload o) (Update o) -> a -> IO (Either a a)) -> a ->
  VersionedObject o -> IO a
foldUpdates ts accessSpace f initv obj = foldObject obj initv where
  foldObject (VersionedObject _ nodeVar) a = do
    node <- atomically $ readTVar nodeVar
    case node of
      Join payload base  -> foldBase (f $ Left payload) base a
      Update update base -> foldBase (f $ Right update) base a

  foldBase g base a = do
    case base of
      None -> either id id `fmap` g a
      Unlinked obj -> step (g a) (foldObject obj)
      Committed creationSpace histVar -> do
        visible <- isVisible ts creationSpace accessSpace
        let m = if visible then g a else return (Left a)
        step m $ \a' -> do
          hist <- atomically $ readTVar histVar
          let obj = histLookup ts hist
          foldObject obj a'

  step m k = do
    res <- m
    case res of
      Left a'  -> k a'
      Right a' -> return a'

-- | Checks if the creator-space is a visible alias of the
--   accessor-space.
isVisible :: TS -> RSpace o -> RSpace o -> IO Bool
isVisible ts creator@(RSpace crUnq _) (RSpace acUnq accessorVar)
  | crUnq == acUnq = return True
  | otherwise      = do
      acShared <- atomically $ readTVar accessorVar
      case acShared of
        SpaceUncommitted _ -> error "isVisible: an object may not be shared with a different space"
        SpaceCommitted _ aliassesVar -> do
          aliassesHist <- atomically $ readTVar aliassesVar
          let aliasses = histLookup ts aliassesHist
              visible  = creator `Set.member` aliasses
          return visible
