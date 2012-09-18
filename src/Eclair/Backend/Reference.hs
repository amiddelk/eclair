-- | A reference/proof-of-concept implementation of @Eclair.Frontend@
--   in a simplified setting, using in-memory concurrent transactions.

{-# LANGUAGE TypeFamilies, GADTs, EmptyDataDecls #-}
module Eclair.Backend.Reference where

import Control.Applicative
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.HashTable(HashTable)
import qualified Data.HashTable as HashTable
import Data.Int
import Data.IntMap(IntMap)
import qualified Data.IntMap as IntMap
import Data.IORef
import Data.List
import Data.Map(Map)
import qualified Data.Map as Map
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
      !( TVar (ObjHist o) )

-- | Ordered sequence of links to a base object.
type ObjHist o = IntMap (VersionedObject o)

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
      -- | A write-lock on the space
      !( TMVar () )

      -- | The objects assigned to the space
      !( TVar (ObjHist o) )

      -- | The aliasses (versioned as well)
      !( TVar (AliassesHist o) )
  | SpaceUncommitted

-- | Ordered sequence of aliasses.
type AliassesHist o = IntMap (Set (RSpace o))


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
  { txnStartTime   :: !TS
  , txnCommitTime  :: !( TVar (Maybe TS) )
  , txnPublish     :: !( HashTable AnyRef RootObject )
  }

-- | A root object of a space, of which the actual type is hidden (existential).
data RootObject :: * where
  SomeRoot :: (vo ~ VersionedObject o, IsRoot vo) => !vo -> RootObject

instance IsStore RStore where
  type Ref RStore   = RRef
  type Trans RStore = RTrans

  openTransaction  s = do 
    ts  <- readClock s
    var <- newTVarIO Nothing
    updates <- HashTable.new (==) hashRRef
    return $ RTrans
      { txnStartTime  = ts
      , txnCommitTime = var
      , txnPublish    = updates
      }

  abortTransaction _ _ = return ()

  commitTransaction s txn = do
    -- obtain the spaces to commit (ordered)
    published0 <- HashTable.toList $ txnPublish txn
    let comparePublish (a,_) (b,_) = compare a b
        published = sortBy comparePublish $ published0

    -- get exclusive access to spaces
    let recoverSpaces = atomically . mapM_ recoverSpace
        recoverSpace (PubCommitted _ lockVar _ _) = do
          restoreNeeded <- isEmptyTMVar lockVar
          when restoreNeeded $ putTMVar lockVar ()
        recoverSpace _ = return ()
        grabSpaces = atomically $ forM published $ grabSpace

        grabSpace :: (AnyRef, RootObject) -> STM PublishInfo
        grabSpace (ref, root) =
          case ref of
            RRef (RSpace _ stateVar) -> do
              state <- readTVar stateVar
              return (undefined, root)
              case state of
                SpaceUncommitted -> do
                  -- uncommitted, thus already exclusive to txn
                  return $ PubUncommitted ref stateVar root
                SpaceCommitted lockVar histVar _ -> do
                  ()   <- takeTMVar lockVar
                  return $ PubCommitted ref lockVar histVar root

    bracketOnError grabSpaces recoverSpaces $ \pubs -> do
      -- incr+get commit time
      ts <- incrementClock s
      atomically $ writeTVar (txnCommitTime txn) $! Just ts

      let -- if committed: update the root to the current state of the space
          -- if uncommitted: register the update nodes with the space
          processSpace :: PublishInfo -> IO ()
          processSpace (PubUncommitted (RRef spUntyped) spaceVarUntyped (SomeRoot patch)) = do
            let sp       = unsafeCoerce spUntyped
                spaceVar = unsafeCoerce spaceVarUntyped
            registerSpace sp ts patch
            shared <- initializeSpace ts sp patch
            atomically $ writeTVar spaceVar $! shared
          processSpace (PubCommitted refUntyped@(RRef spUntyped) lockVar histVarUntyped (SomeRoot patch)) = do
            -- coerce from the "whatever" type to the type of patch
            let histVar = unsafeCoerce histVarUntyped
                ref     = unsafeCoerce refUntyped
                sp      = unsafeCoerce spUntyped
            hist <- atomically $ readTVar histVar
            let current = histLookup ts hist
            patch' <- joinRoots s txn ref patch current
            let hist' = histInsert ts patch' hist
            atomically $ writeTVar histVar $! hist'
            atomically $ putTMVar lockVar ()

      -- process and release the spaces one by one
      forM_ pubs processSpace

data PublishInfo
  = PubUncommitted  !AnyRef !(TVar (SpaceShared Whatever))  !RootObject
  | PubCommitted    !AnyRef !(TMVar ())  !(TVar (ObjHist Whatever))  !RootObject

 
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


class VersionedJoin o where
  joinObjects :: TS -> RSpace o -> VersionedObject o -> VersionedObject o -> IO (VersionedObject o)

instance VersionedJoin o => IsRoot (VersionedObject o) where
  accessSpace s txn ref = do
    -- find a locally updated version, if any.
    mbLocal <- lookupLocal txn ref
    case mbLocal of
      Just obj -> return obj
      Nothing  ->
        -- get the appropriate global version
        case ref of
          RRef space -> lookupSpace txn space

  allocSpace s txn obj = do
    spaceVar <- newTVarIO $ SpaceUncommitted
    u <- newUnique
    let space = RSpace u spaceVar
        ref   = RRef $! space
    registerPublish txn ref obj
    return ref

  updateSpace s txn ref obj =
    registerPublish txn ref obj

  joinRoots _ txn (RRef space) new current = do
    Just ts <- atomically $ readTVar $ txnCommitTime txn
    joinObjects ts space new current

-- | The history must contain a mapping with
--   a key smaller or equal to the @ts@.
histLookup :: TS -> IntMap t -> t
histLookup ts hist = 
  case IntMap.splitLookup ts hist of
    (smaller, mbObj, _) ->
      case mbObj of
        Nothing  -> snd $ IntMap.findMax smaller
        Just obj -> obj

-- | Adds a pair ts obj to the history
histInsert :: TS -> t -> IntMap t -> IntMap t
histInsert = IntMap.insert

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
    SpaceCommitted _ histVar _ -> do
      hist <- atomically $ readTVar histVar
      let ts = txnStartTime txn
      return $! histLookup ts hist 
    SpaceUncommitted -> error "lookupSpace: cannot be called on an uncommitted space"

-- | Registers the mapping from reference to object with the transaction.
registerPublish :: (IsRoot r, r ~ VersionedObject o) => RTrans -> RRef r -> r -> IO ()
registerPublish txn ref obj = do
  let tbl = txnPublish txn
      key = unsafeCoerce ref
      val = SomeRoot obj
  void $ HashTable.update tbl key val

-- * Those pesky type parameters

data Whatever :: *
type AnyRef    = RRef AnyObject
type AnyObject = VersionedObject Whatever


-- * Space functionality

initializeSpace :: TS -> RSpace o -> VersionedObject o -> IO (SpaceShared o)
initializeSpace ts sp obj = do
  let objHist   = IntMap.singleton ts obj
  let aliasHist = IntMap.singleton ts $! Set.singleton sp
  objVar   <- newTVarIO $! objHist
  aliasVar <- newTVarIO $! aliasHist
  lockVar  <- newTMVarIO ()
  return $ SpaceCommitted lockVar objVar aliasVar

-- | Transforms the states of the versioned objects so
--   that these go from unlinked to committed.
--   Precondition: unique ownershop of the versioned
--   object chain.
registerSpace :: RSpace o -> TS -> VersionedObject o -> IO ()
registerSpace sp ts (VersionedObject _ var) = do
  node <- atomically $ readTVar var

  let register base f =
        case base of
          Unlinked prev -> do
            histVar <- newTVarIO $! IntMap.singleton ts prev
            let base' = Committed sp histVar
                node' = f base'
            atomically $ writeTVar var $! node'
            registerSpace sp ts prev            
          _             -> return ()
          

  case node of
    Join p b   -> register b (Join p)
    Update u b -> register b (Update u)


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
        hist    <- atomically $ readTVar histVar
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
      None         -> either id id `fmap` g a
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
        SpaceUncommitted -> error "isVisible: an object may not be shared with a different space"
        SpaceCommitted _ _ aliassesVar -> do
          aliassesHist <- atomically $ readTVar aliassesVar
          let aliasses = histLookup ts aliassesHist
              visible  = creator `Set.member` aliasses
          return visible
