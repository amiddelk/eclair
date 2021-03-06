-- | A reference/proof-of-concept implementation of @Eclair.Frontend@
--   in a simplified setting, using in-memory concurrent transactions.

{-# LANGUAGE TypeFamilies, GADTs, EmptyDataDecls, Rank2Types, FlexibleInstances, FlexibleContexts #-}
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
  type ObjType (VersionedObject o)  = External o

hashVersionedObject :: VersionedObject o -> Int32
hashVersionedObject (VersionedObject u _) = fromIntegral $ hashUnique u

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
--   TODO: think about this: if chains get rearranged, then perhaps
--   this has consequences for the contents of a payload/update,
--   because these may contain information computed from their tail.
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
      !AnySpace
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
      -- | The objects assigned to the space
      !( TMVar (ObjHist o) )

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

refToAnySpace :: RRef o -> AnySpace
refToAnySpace (RRef sp) = coerceToAny sp

data RTrans = RTrans
  { txnStartTime   :: !TS
  , txnCommitTime  :: !( TVar (Maybe TS) )
  , txnPublish     :: !( HashTable AnyRef RootObject )
  , txnCommitCache :: !( HashTable AnyObject AnyPayload )
  , txnJoinMemo    :: !JoinMemo
  }

-- | A root object of a space, of which the actual type is hidden (existential).
data RootObject :: * where
  SomeRoot :: (vo ~ VersionedObject o, IsRoot vo) => !vo -> RootObject

instance IsStore RStore where
  type Ref RStore   = RRef
  type Trans RStore = RTrans

  openTransaction  s = do 
    ts       <- readClock s
    var      <- newTVarIO Nothing
    updates  <- HashTable.new (==) hashRRef
    payloads <- HashTable.new (==) hashVersionedObject
    joinMemo <- joinMemoInitialize
    return $ RTrans
      { txnStartTime    = ts
      , txnCommitTime   = var
      , txnPublish      = updates
      , txnCommitCache  = payloads
      , txnJoinMemo     = joinMemo
      }

  abortTransaction _ _ = return ()

  commitTransaction s txn = do
    -- obtain the spaces to commit (ordered)
    published0 <- HashTable.toList $ txnPublish txn
    let comparePublish (a,_) (b,_) = compare a b
        published = sortBy comparePublish $ published0

    -- get exclusive access to spaces
    let recoverSpaces = atomically . mapM_ recoverSpace
        recoverSpace (PubCommitted _ hist histVar _) = do
          restoreNeeded <- isEmptyTMVar histVar
          when restoreNeeded $ putTMVar histVar hist
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
                SpaceCommitted histVar _ -> do
                  hist <- takeTMVar histVar
                  return $ PubCommitted ref hist histVar root

    bracketOnError grabSpaces recoverSpaces $ \pubs -> do
      -- incr+get commit time
      ts <- incrementClock s
      atomically $ writeTVar (txnCommitTime txn) $! Just ts

      let -- if committed: update the root to the current state of the space
          -- if uncommitted: register the update nodes with the space
          processSpace :: PublishInfo -> IO ()
          processSpace (PubUncommitted refUntyped@(RRef spUntyped) spaceVarUntyped (SomeRoot patch)) = do
            let sp       = coerceFromAny spUntyped
                spaceVar = coerceFromAny' spaceVarUntyped
                ref      = coerceFromAnyObj refUntyped
            patch' <- joinRoots s txn ref patch Nothing
            shared <- initializeSpace ts sp patch'
            atomically $ writeTVar spaceVar $! shared
          processSpace (PubCommitted refUntyped@(RRef spUntyped) histUntyped histVarUntyped (SomeRoot patch)) = do
            let histVar = coerceFromAny' histVarUntyped
                hist    = coerceFromAnyObj histUntyped
                ref     = coerceFromAnyObj refUntyped
                sp      = coerceFromAny spUntyped
                current = histLookup ts hist
            patch' <- joinRoots s txn ref patch $ Just current
            let hist' = histInsert ts patch' hist
            atomically $ putTMVar histVar $! hist'

      -- process and release the spaces one by one
      forM_ pubs processSpace

data PublishInfo
  = PubUncommitted !AnyRef !(TVar AnySpaceShared) !RootObject
  | PubCommitted   !AnyRef !AnyObjHist !(TMVar AnyObjHist) !RootObject

 
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

-- | Context information for type-specific join functions.
--   To recurse on their subcomponents, the functions
--   @jiRegister@ and @jiJoin@ should be used.
data JoinInfo = JoinInfo
  { jiTxn      :: !RTrans
  , jiStore    :: !RStore
  , jiSpace    :: !AnySpace    -- the space (untyped)
  , jiTS       :: !TS          -- timestamp of the commit
  , jiJoin     :: !( forall o . IsPatch o => VersionedObject o -> Maybe (VersionedObject o)
                                          -> IO (VersionedObject o) )
  }

data ViewInfo = ViewInfo
  { viTxn     :: !RTrans
  , viMbSpace :: !(Maybe AnySpace)
  , viTS      :: !TS
  }

jiToVi :: JoinInfo -> ViewInfo
jiToVi (JoinInfo { jiTxn = txn, jiSpace = sp, jiTS = ts }) =
  ViewInfo { viTxn = txn, viMbSpace = Just sp, viTS = ts }

objToVi :: Obj (VersionedObject o) -> ViewInfo
objToVi (Obj { objCtx = ctx, objRef = mbRef }) = vi where
  txn     = ctxTrans ctx
  mbSpace = fmap refToAnySpace mbRef
  ts      = txnStartTime txn
  vi      = ViewInfo { viTxn = txn, viMbSpace = mbSpace, viTS = ts }

-- | Defines type-specific join functions.
class IsPatch o where
  -- | A payload is a terminator of a sequence of patches,
  --   and generally represents the 'initial pach' or
  --   'initial state'.
  --   A payload thus must appear at the end of a series
  --   of patches, but may occur interspered in the chain
  --   as well, in which case each occurrence is withness
  --   of the merge of two distinct series of patches.
  type Payload o :: *

  -- | An update represents a single patch.
  type Update o :: *

  -- | The external representation of the patch.
  type External o :: *

  -- | Makes the payload ready for move outside the transaction.
  --   If the payload is a non-flat structure, it may need to call join-functions on its
  --   children. The payload may be on top of another object.
  joinPayload :: JoinInfo -> VersionedObject o -> Payload o -> Maybe (VersionedObject o) -> IO (Payload o)

  -- | Makes the update ready for move outside the transaction, potentially updating it with
  --   information from a more recent version.
  joinUpdate  :: JoinInfo -> VersionedObject o -> Update o -> Maybe (VersionedObject o) -> IO (Update o)

-- | Generic implementation for the vesioned object space given type-specific
--   join functions.
instance IsPatch o => IsRoot (VersionedObject o) where
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

  joinRoots s txn (RRef space) new mbCurrent = do
    Just ts <- getCommitTS txn
    let info = JoinInfo
          { jiTxn      = txn, jiStore = s
          , jiSpace    = coerceToAny space
          , jiTS       = ts
          , jiJoin     = joinVersionedObjects info
          }
    jiJoin info new mbCurrent

getCommitTS :: RTrans -> IO (Maybe TS)
getCommitTS txn = atomically $ readTVar $ txnCommitTime txn

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
  let published  = txnPublish txn
      refUntyped = coerceToAnyObj ref
  mbRoot <- HashTable.lookup published refUntyped
  case mbRoot of
    Nothing -> return Nothing
    Just (SomeRoot objUntyped) ->
      let obj   = coerceFromAny' objUntyped
          mbObj = Just $! obj
      in return $ mbObj

-- | Finds the object in the space as observed by the given transaction.
lookupSpace :: RTrans -> RSpace o -> IO (VersionedObject o)
lookupSpace txn (RSpace _ sharedVar) = do
  content <- atomically $ readTVar sharedVar
  case content of
    SpaceCommitted histVar _ -> do
      hist <- atomically $ readTMVar histVar
      let ts = txnStartTime txn
      return $! histLookup ts hist 
    SpaceUncommitted -> error "lookupSpace: cannot be called on an uncommitted space"

-- | Registers the mapping from reference to object with the transaction.
registerPublish :: (IsRoot r, r ~ VersionedObject o) => RTrans -> RRef r -> r -> IO ()
registerPublish txn ref obj = do
  let tbl        = txnPublish txn
      refUntyped = coerceToAnyObj ref
      val        = SomeRoot obj
  void $ HashTable.update tbl refUntyped val

-- * Those pesky type parameters
--
-- The maps and tables explicitly forget type information. The following
-- unsafe functions allow us to regain that information again.

type AnyRef         = RRef AnyObject
type AnyObject      = VersionedObject Whatever
type AnySpace       = RSpace Whatever
type AnySpaceShared = SpaceShared Whatever
type AnyObjHist     = ObjHist Whatever
type AnyPayload     = Payload Whatever

-- | Some variants of @unsafeCoerce@ that can only be used in combination
--   with certain types, in order not to loose too much type information
--   and keep some of the static checking.
class AnyCoerce f where
  coerceToAny :: f t -> f Whatever
  coerceToAny = unsafeCoerce

  coerceToAnyObj :: f (VersionedObject t) -> f (VersionedObject Whatever)
  coerceToAnyObj = unsafeCoerce

  coerceFromAny :: f Whatever -> f t
  coerceFromAny = unsafeCoerce

  coerceFromAny' :: f a -> f b
  coerceFromAny' = unsafeCoerce

  coerceFromAnyObj :: f (VersionedObject Whatever) -> f (VersionedObject t)
  coerceFromAnyObj = unsafeCoerce

instance AnyCoerce RRef
instance AnyCoerce VersionedObject
instance AnyCoerce RSpace
instance AnyCoerce SpaceShared
instance AnyCoerce IntMap
instance AnyCoerce TMVar
instance AnyCoerce TVar


-- * Space functionality

initializeSpace :: TS -> RSpace o -> VersionedObject o -> IO (SpaceShared o)
initializeSpace ts sp obj = do
  let objHist   = IntMap.singleton ts obj
  let aliasHist = IntMap.singleton ts $! Set.singleton sp
  objVar   <- newTMVarIO $! objHist
  aliasVar <- newTVarIO $! aliasHist
  return $ SpaceCommitted objVar aliasVar

-- | Moves the left-hand side out of the transaction, and updates it with patches
--   of the right-hand side.
joinVersionedObjects :: IsPatch o => JoinInfo -> VersionedObject o ->
                          Maybe (VersionedObject o) -> IO (VersionedObject o)
joinVersionedObjects info = joinCached where
  joinCached left mbRight = do
    let memo = txnJoinMemo $ jiTxn info
    mbCached <- joinMemoLookup memo left mbRight
    case mbCached of
      Nothing -> do
        joinMemoBlackhole memo left mbRight
        res <- joinTop left mbRight
        joinMemoAdd memo left mbRight res
        return res
      Just res -> return res

  joinTop left mbRight = do
    mbFork <- getFork info left mbRight
    let ts = jiTS info
        sp = jiSpace info
    -- foldUpdates is a left-fold, we turn it
    -- into a right fold by building a function
    -- with it that goes from right-to-left.
    Right res <- foldUpdates ts sp (step mbFork mbRight) return left
    return res

  step (Just fork) (Just right) obj _ _ prefixM
    | fork == obj = prefixM right >>= stop  -- reached the fork
  step _ mbRight obj base0 contents prefixM = do
    let constructOnBase mbChild base = do
          let cont node = mkVersionedObject node >>= prefixM
              mbPrev = mbRight >> mbChild
          case contents of
            Left payload0  -> do
              payload' <- joinPayload info obj payload0 mbPrev
              cont $ Join payload' base
            Right update0 -> do
              update' <- joinUpdate info obj update0 mbPrev
              cont $ Update update' base
        constructOnChild child =
          mkBase child >>= constructOnBase (Just child)

    case base0 of
      None ->  -- reached the end of the chain
        case mbRight of
          Nothing    -> constructOnBase Nothing None >>= stop
          Just right -> constructOnChild right       >>= stop
      _    -> continue constructOnChild

  continue prefixM = return $ Left $! prefixM
  stop res = return $ Right $! res

  -- creates a fresh 'base' node with given child
  mkBase obj = do
    let ts   = jiTS info
        hist = IntMap.singleton ts obj
    var <- newTVarIO hist
    let sp   = jiSpace info
        base = Committed sp var
    return $ base

-- | Gets the most-resent point where two patch series fork, if any.
getFork :: JoinInfo -> VersionedObject o -> Maybe (VersionedObject o) -> IO (Maybe (VersionedObject o))
getFork _ _ Nothing = return Nothing
getFork info root (Just right) = do
  patches <- getPatchSet info right
  getCommonPatch info root patches

-- | Gets the most resent patch in a series of patches that is
--   a member of the given set, if any.
getCommonPatch :: JoinInfo -> VersionedObject o -> Set (VersionedObject o) -> IO (Maybe (VersionedObject o))
getCommonPatch info root patches = do
  let ts    = jiTS info
      space = jiSpace info
      step obj base _ isUnlinked
        | obj `Set.member` patches = return $ Right $ Just $! obj
        | otherwise =
            case base of
              None           -> return $ Right Nothing
              Unlinked _     -> return $ Left True
              Committed _ _
                | isUnlinked -> return $ Left False
                | otherwise  -> return $ Right Nothing
  Right res <- foldUpdates ts space step True root
  return res

-- | Gives the set of a series of patches rooted by @root@.
getPatchSet :: JoinInfo -> VersionedObject o -> IO (Set (VersionedObject o))
getPatchSet info root = do
  let ts    = jiTS info
      space = jiSpace info
      f obj _ _ s  = do
        let s' = Set.insert obj s
            r  = Left $! s'
        return $! r
  Left res <- foldUpdates ts space f Set.empty root
  return res

-- | Turns a node into a versiond object.
mkVersionedObject :: Node o -> IO (VersionedObject o)
mkVersionedObject node = do
  u   <- newUnique
  var <- newTVarIO node
  return $ VersionedObject u var

-- | Wraps the payload as a starting point of a patch series.
wrapPayload :: Payload o -> IO (VersionedObject o)
wrapPayload payload = mkVersionedObject $ Join payload None

-- | Create a fresh versioned object by adding an update on
--   top of an existing one.
addUpdateUnlinked :: VersionedObject o -> Update o -> IO (VersionedObject o)
addUpdateUnlinked vo upd = mkVersionedObject $ Update upd $ Unlinked vo


-- * Traversal of updates

buildUpdates ::
  ViewInfo ->
  (VersionedObject o -> Base o ->  Either (Payload o) (Update o) -> Payload o -> IO (Payload o)) ->
  VersionedObject o -> IO (Payload o)
buildUpdates (ViewInfo { viTxn = txn, viMbSpace = mbAccessSpace, viTS = ts }) f root = foldTop root where
  foldTop obj = do
    mbPayload <- payloadMemoLookup txn ts obj
    case mbPayload of
      Nothing -> do
        payload <- foldObject obj
        payloadMemoAdd txn ts obj payload
        return payload
      Just payload -> return payload

  foldObject obj@(VersionedObject _ nodeVar) = do
    node <- atomically $ readTVar nodeVar
    case node of
      Join payload None  -> return payload
      Join payload base  -> foldBase (f obj base $ Left payload) base
      Update update base -> foldBase (f obj base $ Right update) base

  foldBase g base = do
    case base of
      None         -> error "foldBase: encountered a NONE value"
      Unlinked obj -> foldObject obj >>= g
      Committed creationSpace histVar ->
        case mbAccessSpace of
          Nothing -> error "foldBase: encountered a committed value without space"
          Just accessSpace -> do
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
  TS -> AnySpace ->
  (VersionedObject o -> Base o -> Either (Payload o) (Update o) -> a -> IO (Either a b)) -> a ->
  VersionedObject o -> IO (Either a b)
foldUpdates ts accessSpace f initv root = foldObject root initv where
  foldObject obj@(VersionedObject _ nodeVar) a = do
    node <- atomically $ readTVar nodeVar
    case node of
      Join payload base  -> foldBase (f obj base $ Left payload) base a
      Update update base -> foldBase (f obj base $ Right update) base a

  foldBase g base a = do
    case base of
      None         -> g a
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
      Left a' -> k a'
      Right _ -> return res

-- | Checks if the creator-space is a visible alias of the
--   accessor-space.
isVisible :: TS -> AnySpace -> AnySpace -> IO Bool
isVisible ts creator@(RSpace crUnq _) (RSpace acUnq accessorVar)
  | crUnq == acUnq = return True
  | otherwise      = do
      acShared <- atomically $ readTVar accessorVar
      case acShared of
        SpaceUncommitted -> error "isVisible: an object may not be shared with a different space"
        SpaceCommitted _ aliassesVar -> do
          aliassesHist <- atomically $ readTVar aliassesVar
          let aliasses = histLookup ts aliassesHist
              visible  = creator `Set.member` aliasses
          return visible


-- * Join cache
--
-- The join memo is needed for preserving sharing. When a shared @VersionedObject@
-- is joined, it may be required to unsare it, unless all shared instances are
-- joined with the same object. This memo table will cluster those instances
-- that can keep being shared.

type JoinMemo = HashTable AnyObject JoinMemoMap
type JoinMemoMap = Map (Maybe AnyObject) (Maybe AnyObject)

joinMemoInitialize :: IO JoinMemo
joinMemoInitialize = HashTable.new (==) hashVersionedObject

joinMemoLookup :: JoinMemo -> VersionedObject o -> Maybe (VersionedObject o) -> IO (Maybe (VersionedObject o))
joinMemoLookup tbl left mbRight = do
  let mbRightUntyped = fmap coerceToAny mbRight
  mp <- joinMemoLookupMap tbl left
  case Map.lookup mbRightUntyped mp of
    Nothing                  -> return Nothing
    Just Nothing             -> throwIO $ ErrorCall "joinMemoLookup: cyle in object graph"
    Just (Just objUntyped) -> do
      let obj = coerceFromAny objUntyped
      return $ Just $! obj

-- | Assumes that the given binding does not exist yet.
joinMemoAdd :: JoinMemo -> VersionedObject o -> Maybe (VersionedObject o) -> VersionedObject o -> IO ()
joinMemoAdd tbl left mbRight result = do
  let leftUntyped    = coerceToAny left
      mbRightUntyped = fmap coerceToAny mbRight
      resultUntyped  = coerceToAny result
  mp <- joinMemoLookupMap tbl left
  let entry = Just $! resultUntyped
  evaluate entry
  let mp'   = Map.insert mbRightUntyped entry mp
  void $ HashTable.update tbl leftUntyped $! mp'

joinMemoLookupMap :: JoinMemo -> VersionedObject o -> IO JoinMemoMap
joinMemoLookupMap tbl left = do
  let leftUntyped    = coerceToAny left
  maybe Map.empty id <$> HashTable.lookup tbl leftUntyped

joinMemoBlackhole :: JoinMemo -> VersionedObject o -> Maybe (VersionedObject o) -> IO ()
joinMemoBlackhole tbl left mbRight = do
  let leftUntyped    = coerceToAny left
      mbRightUntyped = fmap coerceToAny mbRight
  mp <- joinMemoLookupMap tbl left
  let mp' = Map.insert mbRightUntyped Nothing mp
  void $ HashTable.update tbl leftUntyped $! mp'

-- * Payload cache
--
-- Lookups done during the transaction (which have a ts == txnStartTime) may happen
-- concurrently.
payloadMemoLookup :: RTrans -> TS -> VersionedObject o -> IO (Maybe (Payload o))
payloadMemoLookup txn ts obj
  | txnStartTime txn == ts = return Nothing
  | otherwise = do
      let tbl = txnCommitCache txn
          objUntyped = coerceToAny obj
      mbPayloadUntyped <- HashTable.lookup tbl objUntyped
      case mbPayloadUntyped of
        Nothing             -> return Nothing
        Just payloadUntyped -> return $ Just $! unsafeCoerce payloadUntyped

payloadMemoAdd :: RTrans -> TS -> VersionedObject o -> Payload o -> IO ()
payloadMemoAdd txn ts obj payload
  | txnStartTime txn == ts = return ()
  | otherwise = do
      let tbl            = txnCommitCache txn
          objUntyped     = coerceToAny obj
          payloadUntyped = unsafeCoerce payload
      void $ HashTable.update tbl objUntyped payloadUntyped


-- * Helper functions for data types

unconditionallyUpdate :: Obj (VersionedObject t) -> Update t -> Obj (VersionedObject t)
unconditionallyUpdate (Obj { objValue = vo, objCtx = ctx, objRef = mbRef }) upd =
  performAsyncPure ctx $ \cont -> do
    vo' <- addUpdateUnlinked vo upd
    cont $ wrapObj ctx mbRef $! vo'

unconditionallyWrap :: (IsStore s, ObjStore o ~ s, ObjFam ix s ~ o, o ~ VersionedObject t) => Ctx s -> ix -> Payload t -> Obj o
unconditionallyWrap ctx _ a =
  performAsyncPure ctx $ \cont -> do
    vo <- wrapPayload a
    cont $ wrapObj ctx Nothing $! vo


-- * Implementation of indirections

-- todo


-- * Implementation of a counter

newtype RCounter a = RCounter ()
type instance ObjFam (TCounter a) RStore = VersionedObject (RCounter a)

instance Num a => IsPatch (RCounter a) where
  type Payload  (RCounter a) = a
  type Update   (RCounter a) = a
  type External (RCounter a) = a

  joinPayload _ _ payload _ = return payload
  joinUpdate  _ _ update  _ = return update

instance Num a => HasWrap (VersionedObject (RCounter a)) where
  wrap = unconditionallyWrap

instance Num a => HasView (VersionedObject (RCounter a)) where
  view obj@(Obj { objValue = vo, objCtx = ctx }) =
    performAsyncPure ctx $ \cont -> do
      let vi = objToVi obj
          build _ _ (Left ticks) ticks0  = do
            return (ticks + ticks0)
          build _ _ (Right delta) ticks0 = do
            return (delta + ticks0)
      buildUpdates vi build vo >>= cont

instance Num a => HasIncr (VersionedObject (RCounter a)) where
  incrBy delta obj = unconditionallyUpdate obj delta


-- * Implementation of a dictionary

newtype RDict k v = RDict ()
type instance ObjFam (TDict (IDict k v)) RStore = VersionedObject (RDict k v)

data DictUpdate k v
  = DictUpdate !k !v

forceEvalMap :: Map k v -> IO ()
forceEvalMap = void . evaluate . Map.size

instance (Ord k, IsPatch v) => IsPatch (RDict k (VersionedObject v)) where
  type Payload (RDict k (VersionedObject v))  = Map k (VersionedObject v)
  type Update (RDict k (VersionedObject v))   = DictUpdate k (VersionedObject v)
  type External (RDict k (VersionedObject v)) = Map k (VersionedObject v)

  -- | Defined in terms of @joinUpdate@
  joinPayload info obj payload mbRight = do
    let new = Map.toAscList payload
    new' <- forM new $ \(k, a) -> do
      DictUpdate _ a' <- joinUpdate info obj (DictUpdate k a) mbRight
      return (k,a')
    let payload' = Map.fromAscList new'
    forceEvalMap payload'
    return payload'

  joinUpdate info _ (DictUpdate k v) Nothing = do
    v' <- jiJoin info v Nothing
    return (DictUpdate k v')
  joinUpdate info _ (DictUpdate k v) (Just right) = do
    let vi = jiToVi info
    mp <- buildMap vi right
    let mbCurrent = Map.lookup k mp
    v' <- jiJoin info v mbCurrent
    return (DictUpdate k v')

buildMap :: Ord k => ViewInfo -> VersionedObject (RDict k (VersionedObject v)) -> IO (Map k (VersionedObject v))
buildMap vi = buildUpdates vi f where
  f _ _ (Left mp) mp0 = do
    let res = Map.union mp mp0
    forceEvalMap res
    return res
  f _ _ (Right (DictUpdate k v)) mp0 = do
    let res = Map.insert k v mp0
    forceEvalMap res
    return res

instance IsDictObj (VersionedObject (RDict k v)) where
  type DictKey   (VersionedObject (RDict k v)) = k
  type DictValue (VersionedObject (RDict k v)) = Obj v

instance (Ord k, IsPatch v) => HasUpdateBinding (VersionedObject (RDict k (VersionedObject v))) where
  updateBinding k v m = unconditionallyUpdate m $ DictUpdate k $ objValue v

instance (Ord k, IsPatch v) => HasWrap (VersionedObject (RDict k (VersionedObject v))) where
  wrap = unconditionallyWrap

instance (Ord k) => HasView (VersionedObject (RDict k (VersionedObject v))) where
  view obj@(Obj { objValue = vo, objCtx = ctx }) =
    performAsyncPure ctx $ \cont -> do
      let vi = objToVi obj
      buildMap vi vo >>= cont

instance Ord k => HasLookup (VersionedObject (RDict k (VersionedObject v))) where
  lookup obj@(Obj { objCtx = ctx, objRef = mbRef }) key =
    case Map.lookup key $ view obj of
      Nothing -> Nothing
      Just vo -> Just $! wrapObj ctx mbRef $! vo
